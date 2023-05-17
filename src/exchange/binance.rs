use super::{Command, ExchangeConnection};
use futures_util::{SinkExt, StreamExt};
use itertools::Itertools;
use log::{error, info, trace, warn};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashSet, time::SystemTime};
use tokio::{net::TcpStream, select, sync::mpsc};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const WEBSOCKET_URL: &str = "wss://stream.binance.com:9443/stream";
const CONNECTION_RETRY_INTERVAL_SECONDS: u64 = 1;
const PONG_INTERVAL_SECONDS: u64 = 30;
const PONG_MESSAGE: Message = Message::Pong(vec![]);

#[derive(Debug, Deserialize)]
struct Orderbook {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct OrderbookDiff {
    #[serde(rename = "E")]
    _event_timestamp: u64,
    #[serde(rename = "s")]
    instrument_id: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    last_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

/// Binance websocket message types
#[derive(Debug)]
enum ExchangeMessage {
    /// Orderbook update
    Orderbook(String, Orderbook),

    /// Orderbook diff update
    OrderbookDiff(OrderbookDiff),

    /// Successful subscription message containing the channel name
    SubscriptionSucceeded(String),
}

pub struct BinanceConnection {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl ExchangeConnection for BinanceConnection {
    fn subscribe_orderbook(&mut self, symbol: &str) {
        info!("Subscribing to {} on Binance", symbol);
        self.command_tx
            .send(Command::SubscribeOrderbook(symbol.to_string()))
            .unwrap();
    }

    fn new() -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        tokio::spawn(new_connection_handler(command_rx));
        Self { command_tx }
    }
}

/// Creates a new subscribe message for given channels
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
        Some("depth") => {
            let orderbook_diff: OrderbookDiff = serde_json::from_value(data).ok()?;
            Some(ExchangeMessage::OrderbookDiff(orderbook_diff))
        }
        _ => None,
    }
}

async fn new_message_handler(
    subscribed_channels: &mut HashSet<String>,
    websocket_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    command_rx: &mut mpsc::UnboundedReceiver<Command>,
) {
    let (mut websocket_tx, mut websocket_rx) = websocket_stream.split();

    if !subscribed_channels.is_empty() {
        info!("Resubscribing to channels: {subscribed_channels:?}");
        websocket_tx
            .send(Message::Text(new_subscribe_message(
                subscribed_channels.clone().into_iter().collect_vec(),
            )))
            .await
            .unwrap();
    }

    let mut pong_interval =
        tokio::time::interval(tokio::time::Duration::from_secs(PONG_INTERVAL_SECONDS));

    loop {
        select! {
            _ = pong_interval.tick() => {
                websocket_tx.send(PONG_MESSAGE).await.unwrap();
            }
            Some(command) = command_rx.recv() => {
                match command {
                    Command::SubscribeOrderbook(symbol) => {
                        info!("Subscribing to {symbol} orderbook on Binance");
                        let channels = vec![
                            format!("{symbol}@depth@100ms", symbol=symbol),
                            format!("{symbol}@depth10@100ms", symbol=symbol),
                            format!("{symbol}@depth20@100ms", symbol=symbol)
                        ];
                        channels.iter().for_each(|channel| {
                            subscribed_channels.insert(channel.to_string());
                        });
                        websocket_tx.send(Message::Text(
                            new_subscribe_message(channels)
                        )).await.unwrap();
                    }
                }
            }
            websocket_message = websocket_rx.next() => {
                match websocket_message {
                    Some(Ok(message)) if message.is_text() => {
                        match parse_exchange_message(&message) {
                            Some(message) => {
                                info!("Received message from Binance: {message:?}")
                            },
                            None => {
                                warn!("Unknown message received from Binance: {message:?}");
                            }
                        }
                    }
                    Some(Ok(message)) => {
                        warn!("None text message received from Binance: {message:?}");
                        if message.is_ping() {
                            info!("Ping message: {}", message.to_string());
                        }
                    }
                    Some(Err(e)) => {
                        error!("Error receiving message from Binance: {e}");
                        break;
                    }
                    None => {
                        error!("Binance closed connection");
                        break;
                    }
                }
            }
        }
    }
}

/// Opens and manages websocket (re)connection
async fn new_connection_handler(mut command_rx: mpsc::UnboundedReceiver<Command>) {
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
        new_message_handler(&mut subscribed_channels, websocket_stream, &mut command_rx).await;
    }
}
