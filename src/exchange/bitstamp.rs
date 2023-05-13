use super::ExchangeConnection;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use std::collections::HashSet;
use tokio::{net::TcpStream, select, sync::mpsc};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const WEBSOCKET_URL: &str = "wss://ws.bitstamp.net";
const CONNECTION_RETRY_INTERVAL_SECONDS: u64 = 1;

#[derive(Debug)]
enum Command {
    SubscribeOrderbook(String),
}

pub struct BitstampConnection {
    command_tx: mpsc::UnboundedSender<Command>,
}

impl ExchangeConnection for BitstampConnection {
    fn subscribe_orderbook(&mut self, symbol: &str) {
        info!("Subscribing to {} on Bitstamp", symbol);
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

/// Event loop for handling websocket messages and exchange commands
async fn new_message_handler(
    subscribed_messages: &mut HashSet<String>,
    websocket_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    command_rx: &mut mpsc::UnboundedReceiver<Command>,
) {
    let (mut websocket_tx, mut websocket_rx) = websocket_stream.split();

    for message in subscribed_messages.iter() {
        info!("Resubscribing to {}", message);
        websocket_tx
            .send(Message::Text(message.clone()))
            .await
            .unwrap();
    }

    loop {
        select! {
            Some(command) = command_rx.recv() => {
                match command {
                    Command::SubscribeOrderbook(symbol) => {
                        info!("Subscribing to {symbol} orderbook on Bitstamp");
                        let message = format!(r#"{{"event":"bts:subscribe","data":{{"channel":"diff_order_book_{symbol}"}}}}"#);
                        subscribed_messages.insert(message.clone());
                        websocket_tx.send(Message::Text(message)).await.unwrap();
                    }
                }
            }
            websocket_message = websocket_rx.next() => {
                match websocket_message {
                    Some(Ok(message)) if message.is_text() => {
                        info!("Received message from Bitstamp: {}", message.to_string());
                    }
                    Some(Ok(message)) => {
                        warn!("None text message received from Bitstamp: {}", message.to_string());
                    }
                    Some(Err(e)) => {
                        error!("Error receiving message from Bitstamp: {e}");
                        break;
                    }
                    None => {
                        error!("Bitstamp closed connection");
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
        new_message_handler(&mut subscribed_messages, websocket_stream, &mut command_rx).await;
    }
}
