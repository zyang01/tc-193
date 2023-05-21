use super::{Command, ExchangeConnection};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use itertools::Itertools;
use log::{error, info, warn};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashSet, time::SystemTime};
use tokio::{net::TcpStream, select, sync::mpsc};
use tokio_tungstenite::{
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};

const EXCHANGE_NAME: &str = "binance";
const WEBSOCKET_URL: &str = "wss://stream.binance.com:9443/stream";
const CONNECTION_RETRY_INTERVAL_SECONDS: u64 = 1;
const PONG_INTERVAL_SECONDS: u64 = 30;
const PONG_MESSAGE: Message = Message::Pong(vec![]);

/// Orderbook update
#[derive(Debug, Deserialize)]
struct Orderbook {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,

    /// Bids represented as `[[price, amount], ...]`
    bids: Vec<[String; 2]>,

    /// Asks represented as `[[price, amount], ...]`
    asks: Vec<[String; 2]>,
}

impl Into<super::Orderbook> for Orderbook {
    /// Converts orderbook to `super::Orderbook`
    fn into(self) -> super::Orderbook {
        super::Orderbook {
            monotonic_counter: self.last_update_id,
            bids: self.bids,
            asks: self.asks,
        }
    }
}

/// Binance websocket message types
#[derive(Debug)]
enum ExchangeMessage {
    /// Orderbook update
    Orderbook(String, Orderbook),
}

impl Into<super::ExchangeMessage> for ExchangeMessage {
    /// Converts exchange message to `super::ExchangeMessage`
    fn into(self) -> super::ExchangeMessage {
        match self {
            ExchangeMessage::Orderbook(instrument_id, orderbook) => {
                super::ExchangeMessage::Orderbook(
                    EXCHANGE_NAME.to_string(),
                    instrument_id,
                    orderbook.into(),
                )
            }
        }
    }
}

/// Binance websocket connection
pub struct BinanceConnection {
    /// Channel to send commands to
    command_tx: mpsc::UnboundedSender<Command>,
}

impl ExchangeConnection for BinanceConnection {
    /// Subscribes to orderbook updates for given instrument
    ///
    /// # Arguments
    /// * `instrument_id` - Instrument id
    ///
    /// # Panics
    /// Panics if `command_tx` is closed
    fn subscribe_orderbook(&mut self, instrument_id: &str) {
        info!("Subscribing to {instrument_id} on Binance");
        self.command_tx
            .send(Command::SubscribeOrderbook(instrument_id.to_string()))
            .unwrap();
    }

    fn new(exchange_message_tx: mpsc::UnboundedSender<super::ExchangeMessage>) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        tokio::spawn(new_connection_handler(command_rx, exchange_message_tx));
        Self { command_tx }
    }
}

/// Creates a new subscribe message for given channels
///
/// # Arguments
/// * `channels` - Channels to subscribe to
///
/// # Returns
/// * `String` - Subscribe message
///
/// # Panics
/// Panics if clock is set to before UNIX epoch
fn new_subscribe_message(channels: Vec<String>) -> String {
    json!({
        "method": "SUBSCRIBE",
        "params": channels,
        "id": SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    })
    .to_string()
}

/// Converts websocket message to ExchangeMessage
///
/// # Arguments
/// * `message` - Websocket message
///
/// # Returns
/// * `Option<ExchangeMessage>` - Exchange message
fn parse_exchange_message(message: &Message) -> Option<ExchangeMessage> {
    let mut message: serde_json::Value = serde_json::from_str(message.to_text().ok()?).ok()?;
    let data = message["data"].take();
    let mut stream = message.get("stream")?.as_str()?.split("@");
    let instrument_id = stream.next();
    let stream_name = stream.next();
    match stream_name {
        Some("depth10") | Some("depth20") => {
            let orderbook: Orderbook = serde_json::from_value(data).ok()?;
            Some(ExchangeMessage::Orderbook(
                instrument_id?.to_string(),
                orderbook,
            ))
        }
        _ => None,
    }
}

/// Processes exchange command
///
/// # Arguments
/// * `command` - Exchange command
/// * `websocket_tx` - Websocket sink
/// * `subscribed_channels` - Set of subscribed channels
///
/// # Returns
/// * `Result<(), tungstenite::Error>` - Result
async fn process_exchange_command(
    command: Command,
    websocket_tx: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    subscribed_channels: &mut HashSet<String>,
) -> Result<(), tungstenite::Error> {
    match command {
        Command::SubscribeOrderbook(instrument_id) => {
            info!("Subscribing to {instrument_id} orderbook on Binance");
            let channels = vec![
                // format!("{instrument_id}@depth@100ms"),
                format!("{instrument_id}@depth10@100ms"),
                // format!("{instrument_id}@depth20@100ms"),
            ];
            channels.iter().for_each(|channel| {
                subscribed_channels.insert(channel.to_string());
            });
            websocket_tx
                .send(Message::Text(new_subscribe_message(channels)))
                .await
        }
    }
}

/// Processes websocket message
///
/// # Arguments
/// * `websocket_message` - Websocket message
/// * `exchange_message_tx` - Channel to send exchange messages to
///
/// # Returns
/// * `Result<(), tungstenite::Error>` - Result
///
/// # Panics
/// Panics if `exchange_message_tx` is closed
async fn process_websocket_message(
    websocket_message: Option<Result<Message, tungstenite::Error>>,
    exchange_message_tx: &mut mpsc::UnboundedSender<super::ExchangeMessage>,
) -> Result<(), tungstenite::Error> {
    match websocket_message {
        Some(Ok(message)) if message.is_text() => {
            match parse_exchange_message(&message) {
                Some(message) => exchange_message_tx.send(message.into()).unwrap(),
                None => info!("Message received from Binance: {message}"),
            }
            Ok(())
        }
        Some(Ok(message)) => {
            if message.is_ping() {
                info!("Ping message: {}", message.to_string());
            } else {
                warn!("None text message received from Binance: {message:?}");
            }
            Ok(())
        }
        Some(Err(e)) => Err(e),
        None => Err(tungstenite::Error::ConnectionClosed),
    }
}

/// Event loop for handling websocket messages and exchange commands.
/// Terminates when websocket connection is closed or an error occurs
///
/// # Arguments
/// * `subscribed_channels` - Set of subscribed channels
/// * `websocket_stream` - Websocket stream
/// * `command_rx` - Channel to receive commands from
/// * `exchange_message_tx` - Channel to send exchange messages to
async fn new_message_handler(
    subscribed_channels: &mut HashSet<String>,
    websocket_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    command_rx: &mut mpsc::UnboundedReceiver<Command>,
    exchange_message_tx: &mut mpsc::UnboundedSender<super::ExchangeMessage>,
) {
    let (mut websocket_tx, mut websocket_rx) = websocket_stream.split();

    if !subscribed_channels.is_empty() {
        info!("Resubscribing to channels: {subscribed_channels:?}");
        if let Err(e) = websocket_tx
            .send(Message::Text(new_subscribe_message(
                subscribed_channels.clone().into_iter().collect_vec(),
            )))
            .await
        {
            error!("Error resubscribing to channels: {e}");
            return;
        }
    }

    let mut pong_interval =
        tokio::time::interval(tokio::time::Duration::from_secs(PONG_INTERVAL_SECONDS));

    loop {
        select! {
            _ = pong_interval.tick() => {
                if let Err(e) = websocket_tx.send(PONG_MESSAGE).await {
                    error!("Error sending pong message: {e}");
                    return;
                }
            }
            Some(command) = command_rx.recv() => {
                if let Err(e) = process_exchange_command(command, &mut websocket_tx, subscribed_channels).await {
                    error!("Error processing exchange command: {e}");
                    return
                }
            }
            websocket_message = websocket_rx.next() => {
                if let Err(e) = process_websocket_message(websocket_message, exchange_message_tx).await {
                    error!("Error processing websocket message: {e}");
                    return
                }
            }
        }
    }
}

/// Opens and manages websocket (re)connection
///
/// # Arguments
/// * `command_rx` - Channel to receive commands from
/// * `exchange_message_tx` - Channel to send exchange messages to
///
/// # Panics
/// Panics if `WEB_SOCKET_URL` cannot be parsed as a url
async fn new_connection_handler(
    mut command_rx: mpsc::UnboundedReceiver<Command>,
    mut exchange_message_tx: mpsc::UnboundedSender<super::ExchangeMessage>,
) {
    let websocket_url = url::Url::parse(WEBSOCKET_URL).unwrap();
    let mut subscribed_channels: HashSet<String> = HashSet::new();
    loop {
        let websocket_stream = match tokio_tungstenite::connect_async(websocket_url.clone()).await {
            Ok((stream, response)) => {
                info!("Connected to Binance: {response:?}");
                stream
            }
            Err(e) => {
                error!(
                    "Error connecting to Binance: {e}, retrying in {CONNECTION_RETRY_INTERVAL_SECONDS} seconds"
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    CONNECTION_RETRY_INTERVAL_SECONDS,
                ))
                .await;
                continue;
            }
        };
        new_message_handler(
            &mut subscribed_channels,
            websocket_stream,
            &mut command_rx,
            &mut exchange_message_tx,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example_orderbook_data() -> String {
        json!({
            "stream": "btcusdt@depth10@100ms",
            "data": {
                "lastUpdateId": 36919621358_i64,
                "bids": [
                    [
                        "26761.47000000",
                        "0.05065000"
                    ],
                    [
                        "26761.18000000",
                        "0.00069000"
                    ],
                    [
                        "26761.06000000",
                        "0.00038000"
                    ],
                    [
                        "26760.96000000",
                        "0.35058000"
                    ],
                    [
                        "26760.81000000",
                        "0.00069000"
                    ],
                    [
                        "26760.68000000",
                        "0.00044000"
                    ],
                    [
                        "26760.58000000",
                        "0.00067000"
                    ],
                    [
                        "26760.30000000",
                        "1.57365000"
                    ],
                    [
                        "26760.00000000",
                        "0.09339000"
                    ],
                    [
                        "26759.55000000",
                        "0.00200000"
                    ]
                ],
                "asks": [
                    [
                        "26761.48000000",
                        "5.97881000"
                    ],
                    [
                        "26761.51000000",
                        "0.68135000"
                    ],
                    [
                        "26761.56000000",
                        "0.93394000"
                    ],
                    [
                        "26761.92000000",
                        "0.37271000"
                    ],
                    [
                        "26762.07000000",
                        "0.78350000"
                    ],
                    [
                        "26762.08000000",
                        "0.77330000"
                    ],
                    [
                        "26762.29000000",
                        "0.00069000"
                    ],
                    [
                        "26762.30000000",
                        "0.09047000"
                    ],
                    [
                        "26762.37000000",
                        "0.06165000"
                    ],
                    [
                        "26762.38000000",
                        "0.16147000"
                    ]
                ]
            }
        })
        .to_string()
    }

    #[test]
    fn can_parse_exchange_message() {
        let websocket_message = example_orderbook_data();

        if let Some(ExchangeMessage::Orderbook(instrument_id, orderbook)) =
            parse_exchange_message(&websocket_message.into())
        {
            assert_eq!(instrument_id, "btcusdt");
            assert_eq!(orderbook.last_update_id, 36919621358);

            assert_eq!(orderbook.bids.len(), 10);
            assert_eq!(orderbook.bids[0], ["26761.47000000", "0.05065000"]);

            assert_eq!(orderbook.asks.len(), 10);
            assert_eq!(orderbook.asks[0], ["26761.48000000", "5.97881000"]);
        } else {
            panic!("Could not parse exchange message")
        }
    }

    #[test]
    fn cannot_parse_exchange_message() {
        let websocket_message = json!({
            "result": null,
            "id": 1
        })
        .to_string();

        assert!(parse_exchange_message(&websocket_message.into()).is_none());
    }

    #[test]
    fn can_convert_orderbook() {
        if let Some(ExchangeMessage::Orderbook(instrument_id, orderbook)) =
            parse_exchange_message(&example_orderbook_data().into())
        {
            let orderbook: super::super::Orderbook = orderbook.into();

            assert_eq!(instrument_id, "btcusdt");
            assert_eq!(orderbook.monotonic_counter, 36919621358);

            assert_eq!(orderbook.bids.len(), 10);
            assert_eq!(orderbook.bids[0], ["26761.47000000", "0.05065000"]);

            assert_eq!(orderbook.asks.len(), 10);
            assert_eq!(orderbook.asks[0], ["26761.48000000", "5.97881000"]);
        } else {
            panic!("Could not parse exchange message")
        }
    }
}
