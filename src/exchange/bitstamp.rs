use super::{Command, ExchangeConnection};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use log::{error, info, warn};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use tokio::{net::TcpStream, select, sync::mpsc};
use tokio_tungstenite::{
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};

const EXCHANGE_NAME: &str = "bitstamp";
const WEBSOCKET_URL: &str = "wss://ws.bitstamp.net";
const CONNECTION_RETRY_INTERVAL_SECONDS: u64 = 1;
const HEARTBEAT_INTERVAL_SECONDS: u64 = 5;

/// Orderbook update
#[derive(Debug, Deserialize)]
struct Orderbook {
    microtimestamp: String,

    /// Bids represented as `[[price, amount], ...]`
    bids: Vec<[String; 2]>,

    /// Asks represented as `[[price, amount], ...]`
    asks: Vec<[String; 2]>,
}

impl Into<super::Orderbook> for Orderbook {
    /// Converts orderbook to `super::Orderbook`
    ///
    /// # Panics
    /// Panics if `microtimestamp` cannot be parsed as `u64`
    fn into(self) -> super::Orderbook {
        let microtimestamp = self.microtimestamp.parse::<u64>().unwrap();
        super::Orderbook {
            monotonic_counter: microtimestamp,
            bids: self.bids,
            asks: self.asks,
        }
    }
}

/// Bitstamp websocket message types
#[derive(Debug)]
enum ExchangeMessage {
    /// Orderbook update
    Orderbook(String, Orderbook),
}

impl Into<super::ExchangeMessage> for ExchangeMessage {
    /// Converts websocket message to `super::ExchangeMessage`
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

/// Bitstamp websocket connection
pub struct BitstampConnection {
    /// Channel to send commands to
    command_tx: mpsc::UnboundedSender<Command>,
}

impl ExchangeConnection for BitstampConnection {
    /// Subscribes to orderbook updates for an instrument id
    ///
    /// # Arguments
    /// * `instrument_id` - Instrument id to subscribe to
    ///
    /// # Panics
    /// Panics if `command_tx` is closed
    fn subscribe_orderbook(&mut self, instrument_id: &str) {
        info!("Subscribing to {instrument_id} on Bitstamp");
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
    let event = message.get("event")?.as_str()?;
    let channel = message.get("channel")?.as_str()?;
    match event {
        "data" => {
            let orderbook: Orderbook = serde_json::from_value(data).ok()?;
            let instrument_id = channel.split("_").last()?.to_string();
            Some(ExchangeMessage::Orderbook(instrument_id, orderbook))
        }
        _ => None,
    }
}

/// Processes exchange commands
///
/// # Arguments
/// * `command` - Exchange command
/// * `websocket_tx` - Websocket sink
/// * `subscribed_messages` - Set of subscribed messages
///
/// # Returns
/// * `Result<(), tungstenite::Error>` - Result
async fn process_exchange_command(
    command: Command,
    websocket_tx: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    subscribed_messages: &mut HashSet<String>,
) -> Result<(), tungstenite::Error> {
    match command {
        Command::SubscribeOrderbook(instrument_id) => {
            info!("Subscribing to {instrument_id} orderbook on Bitstamp");
            let message = json!({
                "event": "bts:subscribe",
                "data": {
                    "channel": format!("order_book_{instrument_id}")
                }
            })
            .to_string();
            subscribed_messages.insert(message.clone());
            websocket_tx.send(Message::Text(message)).await
        }
    }
}

/// Processes websocket messages
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
                None => info!("Message received from Bitstamp: {message}"),
            }
            Ok(())
        }
        Some(Ok(message)) => {
            warn!("None text message received from Bitstamp: {:?}", message);
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
    subscribed_messages: &mut HashSet<String>,
    websocket_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    command_rx: &mut mpsc::UnboundedReceiver<Command>,
    exchange_message_tx: &mut mpsc::UnboundedSender<super::ExchangeMessage>,
) {
    let (mut websocket_tx, mut websocket_rx) = websocket_stream.split();

    for message in subscribed_messages.iter() {
        info!("Resubscribing to {}", message);
        if let Err(e) = websocket_tx.send(Message::Text(message.clone())).await {
            error!("Error resubscribing to {message}: {e}");
            return;
        }
    }

    let mut heartbeat_interval =
        tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS));

    loop {
        select! {
            _ = heartbeat_interval.tick() => {
                let message = json!({
                    "event": "bts:heartbeat",
                })
                .to_string();

                if let Err(e) = websocket_tx.send(Message::Text(message)).await {
                    error!("Error sending heartbeat: {e}");
                    return;
                }
            }
            Some(command) = command_rx.recv() => {
                if let Err(e) = process_exchange_command(command, &mut websocket_tx, subscribed_messages).await {
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
    let mut subscribed_messages: HashSet<String> = HashSet::new();
    loop {
        let websocket_stream = match tokio_tungstenite::connect_async(websocket_url.clone()).await {
            Ok((stream, response)) => {
                info!("Connected to Bitstamp: {response:?}");
                stream
            }
            Err(e) => {
                error!("Error connecting to Bitstamp: {e}, retrying in {CONNECTION_RETRY_INTERVAL_SECONDS} seconds");
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    CONNECTION_RETRY_INTERVAL_SECONDS,
                ))
                .await;
                continue;
            }
        };
        new_message_handler(
            &mut subscribed_messages,
            websocket_stream,
            &mut command_rx,
            &mut exchange_message_tx,
        )
        .await;
    }
}
