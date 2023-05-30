use super::{Command, ExchangeConnection};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use itertools::{EitherOrBoth, Itertools};
use log::{debug, error, info, trace, warn};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};
use tokio::{net::TcpStream, select, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};

const EXCHANGE_NAME: &str = "binance";
const REST_URL: &str = "https://api.binance.com";
const WEBSOCKET_URL: &str = "wss://stream.binance.com:9443/stream";
const CONNECTION_RETRY_INTERVAL_SECONDS: u64 = 1;
const PONG_INTERVAL_SECONDS: u64 = 30;
const PONG_MESSAGE: Message = Message::Pong(vec![]);
const ORDERBOOK_LIMIT: u64 = 100;
const REST_REQUEST_TIMEOUT_SECONDS: u64 = 3;

/// Orderbook update
#[derive(Debug, Clone, Deserialize)]
struct Orderbook {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,

    /// Bids represented as `[[price, amount], ...]`
    bids: Vec<[String; 2]>,

    /// Asks represented as `[[price, amount], ...]`
    asks: Vec<[String; 2]>,
}

impl Orderbook {
    fn merge_diff(&mut self, orderbook_diff: OrderbookDiff) {
        // merge orderbook diff into orderbook
        self.bids = self
            .bids
            .iter()
            .merge_join_by(orderbook_diff.bids.iter(), |bid, bid_diff| {
                bid_diff[0]
                    .parse::<f64>()
                    .unwrap()
                    .partial_cmp(&bid[0].parse::<f64>().unwrap())
                    .unwrap()
            })
            .filter_map(|join_result| match join_result {
                EitherOrBoth::Left(bid) => Some(bid.clone()),
                EitherOrBoth::Right(bid_diff) | EitherOrBoth::Both(_, bid_diff) => {
                    if bid_diff[1].parse::<f64>().unwrap() == 0. {
                        None
                    } else {
                        Some(bid_diff.clone())
                    }
                }
            })
            .collect();

        self.asks = self
            .asks
            .iter()
            .merge_join_by(orderbook_diff.asks.iter(), |ask, ask_diff| {
                ask[0]
                    .parse::<f64>()
                    .unwrap()
                    .partial_cmp(&ask_diff[0].parse::<f64>().unwrap())
                    .unwrap()
            })
            .filter_map(|join_result| match join_result {
                EitherOrBoth::Left(ask) => Some(ask.clone()),
                EitherOrBoth::Right(ask_diff) | EitherOrBoth::Both(_, ask_diff) => {
                    if ask_diff[1].parse::<f64>().unwrap() == 0. {
                        None
                    } else {
                        Some(ask_diff.clone())
                    }
                }
            })
            .collect();

        self.last_update_id = orderbook_diff.last_update_id;
    }
}

/// Orderbook diff update
#[derive(Debug, Deserialize)]
struct OrderbookDiff {
    #[serde(rename = "U")]
    first_update_id: u64,

    #[serde(rename = "u")]
    last_update_id: u64,

    /// Bids represented as `[[price, amount], ...]`
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,

    /// Asks represented as `[[price, amount], ...]`
    #[serde(rename = "a")]
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
    /// Orderbook update represented as `(instrument_id, stream_name, orderbook)`
    Orderbook(String, String, Orderbook),

    /// Orderbook diff update represented as `(instrument_id, stream_name, orderbook_diff)`
    OrderbookDiff(String, String, OrderbookDiff),
}

impl Into<super::ExchangeMessage> for ExchangeMessage {
    /// Converts exchange message to `super::ExchangeMessage`
    fn into(self) -> super::ExchangeMessage {
        match self {
            ExchangeMessage::Orderbook(instrument_id, _stream_name, orderbook) => {
                super::ExchangeMessage::Orderbook(
                    EXCHANGE_NAME.to_string(),
                    instrument_id,
                    orderbook.into(),
                )
            }
            ExchangeMessage::OrderbookDiff(_, _, _) => {
                panic!("Cannot convert OrderbookDiff to ExchangeMessage")
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

/// Gets orderbook depth with given instrument id
///
/// # Arguments
/// * `instrument_id` - Instrument id
///
/// # Returns
/// * `Result<Orderbook, reqwest::Error>` - Result
async fn get_orderbook_depth(instrument_id: String) -> Result<Orderbook, reqwest::Error> {
    Ok(reqwest::Client::new()
        .get(format!(
            "{REST_URL}/api/v3/depth?symbol={}&limit={ORDERBOOK_LIMIT}",
            instrument_id.to_uppercase()
        ))
        .timeout(Duration::from_secs(REST_REQUEST_TIMEOUT_SECONDS))
        .send()
        .await?
        .json()
        .await?)
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
    let stream = message.get("stream")?.as_str()?;
    let mut stream_split = stream.split("@");
    let instrument_id = stream_split.next();
    let stream_name = stream_split.next();
    match stream_name {
        Some("depth10") | Some("depth20") => {
            let orderbook: Orderbook = serde_json::from_value(data).ok()?;
            Some(ExchangeMessage::Orderbook(
                instrument_id?.to_string(),
                stream.to_string(),
                orderbook,
            ))
        }
        Some("depth") => {
            let orderbook_diff: OrderbookDiff = serde_json::from_value(data).ok()?;
            Some(ExchangeMessage::OrderbookDiff(
                instrument_id?.to_string(),
                stream.to_string(),
                orderbook_diff,
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
                format!("{instrument_id}@depth@100ms"),
                format!("{instrument_id}@depth10@100ms"),
                format!("{instrument_id}@depth20@100ms"),
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

/// Processes orderbook diff
/// Merge orderbook diff into orderbook if possible, otherwise cache orderbook diff and request
/// orderbook via get request if not already in progress
///
/// # Arguments
/// * `instrument_id` - Instrument id
/// * `stream_name` - Stream name
/// * `orderbook_diff` - Orderbook diff
/// * `orderbooks_cache` - Orderbook cache
/// * `orderbook_diff_cache` - Orderbook diff cache
/// * `orderbook_get_requests` - Orderbook get requests
async fn process_orderbook_diff(
    instrument_id: String,
    stream_name: String,
    orderbook_diff: OrderbookDiff,
    orderbooks_cache: &mut HashMap<String, Orderbook>,
    orderbook_diff_cache: &mut HashMap<String, Vec<OrderbookDiff>>,
    orderbook_get_requests: &mut HashMap<String, JoinHandle<Result<Orderbook, reqwest::Error>>>,
) {
    if orderbooks_cache
        .get(&stream_name)
        .map_or(false, |orderbook| {
            orderbook.last_update_id + 1 == orderbook_diff.first_update_id
        })
    {
        // merge orderbook diff into orderbook
        let orderbook = orderbooks_cache.get_mut(&stream_name).unwrap();
        orderbook.merge_diff(orderbook_diff);
        trace!(
            "merged orderbook diff into orderbook for {}",
            orderbook.last_update_id
        );
        return;
    }

    // cache orderbook diff
    orderbook_diff_cache
        .entry(stream_name.clone())
        .or_insert_with(Vec::new)
        .push(orderbook_diff);

    // orderbook cache is not up to date, request or wait for orderbook via get request
    match orderbook_get_requests.get_mut(&stream_name) {
        Some(request) if request.is_finished() => {
            if let Ok(Ok(mut orderbook)) = request.await {
                debug!(
                    "orderbook get request finished for {}",
                    orderbook.last_update_id
                );
                // remove request
                orderbook_get_requests.remove(&stream_name);

                let merge_success = orderbook_diff_cache
                    .get_mut(&stream_name)
                    .unwrap()
                    .drain(..)
                    .fold(false, |valid, diff| {
                        if (valid && orderbook.last_update_id + 1 == diff.first_update_id)
                            || (!valid
                                && diff.first_update_id <= orderbook.last_update_id + 1
                                && orderbook.last_update_id < diff.last_update_id)
                        {
                            orderbook.merge_diff(diff);
                            true
                        } else {
                            debug!(
                                "discarding orderbook diff for {}->{}",
                                diff.first_update_id, diff.last_update_id
                            );
                            false
                        }
                    });
                debug!(
                    "merge result: {} {}",
                    merge_success, orderbook.last_update_id
                );
                orderbooks_cache.insert(stream_name, orderbook);
            } else {
                // remove errored request
                orderbook_get_requests.remove(&stream_name);

                // drain cache
                if let Some(diff_cache_vec) = orderbook_diff_cache.get_mut(&stream_name) {
                    diff_cache_vec.drain(..diff_cache_vec.len() - 1);
                }
            }
        }
        Some(_) => {
            // request in progress, do nothing
        }
        _ => {
            orderbook_get_requests.insert(
                stream_name,
                tokio::spawn(get_orderbook_depth(instrument_id)),
            );
        }
    }
}

/// Processes websocket message
///
/// # Arguments
/// * `websocket_message` - Websocket message
/// * `exchange_message_tx` - Channel to send exchange messages to
/// * `orderbooks_cache` - Orderbook cache
/// * `latest_orderbook_update_id` - Latest orderbook update id
/// * `orderbook_diff_cache` - Orderbook diff cache
/// * `orderbook_get_requests` - Orderbook get requests
///
/// # Returns
/// * `Result<(), tungstenite::Error>` - Result
///
/// # Panics
/// Panics if `exchange_message_tx` is closed
async fn process_websocket_message(
    websocket_message: Option<Result<Message, tungstenite::Error>>,
    exchange_message_tx: &mut mpsc::UnboundedSender<super::ExchangeMessage>,
    orderbooks_cache: &mut HashMap<String, Orderbook>,
    latest_orderbook_update_id: &mut HashMap<String, u64>,
    orderbook_diff_cache: &mut HashMap<String, Vec<OrderbookDiff>>,
    orderbook_get_requests: &mut HashMap<String, JoinHandle<Result<Orderbook, reqwest::Error>>>,
) -> Result<(), tungstenite::Error> {
    match websocket_message {
        Some(Ok(message)) if message.is_text() => {
            match parse_exchange_message(&message) {
                Some(ExchangeMessage::OrderbookDiff(
                    instrument_id,
                    stream_name,
                    orderbook_diff,
                )) => {
                    trace!(
                        "orderbook diff received from for {instrument_id} {}->{}",
                        orderbook_diff.first_update_id,
                        orderbook_diff.last_update_id
                    );
                    process_orderbook_diff(
                        instrument_id.to_string(),
                        stream_name.to_string(),
                        orderbook_diff,
                        orderbooks_cache,
                        orderbook_diff_cache,
                        orderbook_get_requests,
                    )
                    .await;

                    if let Some(orderbook) = orderbooks_cache.get(&stream_name) {
                        // check if orderbook is newer than cached orderbook
                        if latest_orderbook_update_id
                            .get(&instrument_id)
                            .map_or(true, |prev_update_id| {
                                orderbook.last_update_id > *prev_update_id
                            })
                        {
                            debug!(
                                "latest orderbook received from {stream_name} for {instrument_id} with update id {}: {:?} {:?}",
                                orderbook.last_update_id, orderbook.bids[0], orderbook.asks[0]
                            );
                            exchange_message_tx
                                .send(
                                    ExchangeMessage::Orderbook(
                                        instrument_id.to_string(),
                                        stream_name.to_string(),
                                        orderbook.clone(),
                                    )
                                    .into(),
                                )
                                .unwrap();

                            latest_orderbook_update_id
                                .insert(instrument_id.clone(), orderbook.last_update_id);
                        }
                    }
                }
                Some(ExchangeMessage::Orderbook(instrument_id, stream_name, orderbook)) => {
                    // check if orderbook is newer than cached orderbook
                    if latest_orderbook_update_id
                        .get(&instrument_id)
                        .map_or(true, |prev_update_id| {
                            orderbook.last_update_id > *prev_update_id
                        })
                    {
                        debug!(
                            "latest orderbook received from {stream_name} for {instrument_id} with update id {}: {:?} {:?}",
                            orderbook.last_update_id, orderbook.bids[0], orderbook.asks[0]
                        );
                        exchange_message_tx
                            .send(
                                ExchangeMessage::Orderbook(
                                    instrument_id.to_string(),
                                    stream_name.to_string(),
                                    orderbook.clone(),
                                )
                                .into(),
                            )
                            .unwrap();

                        latest_orderbook_update_id
                            .insert(instrument_id.clone(), orderbook.last_update_id);
                        orderbooks_cache.insert(stream_name, orderbook);
                    }
                }
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
    // Orderbooks by channel/stream name, with values being the orderbook
    let mut orderbooks_cache: HashMap<String, Orderbook> = HashMap::new();

    // Latest orderbook update id by instrument id
    let mut latest_orderbook_update_id: HashMap<String, u64> = HashMap::new();

    // Orderbook diff cache by channel/stream name, with values being a queue of orderbook diffs
    let mut orderbook_diff_cache: HashMap<String, Vec<OrderbookDiff>> = HashMap::new();

    // Orderbook get requests by channel/stream name, with values being the join handle
    let mut orderbook_get_requests: HashMap<String, JoinHandle<Result<Orderbook, reqwest::Error>>> =
        HashMap::new();

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
                if let Err(e) = process_websocket_message(
                    websocket_message,
                    exchange_message_tx,
                    &mut orderbooks_cache,
                    &mut latest_orderbook_update_id,
                    &mut orderbook_diff_cache,
                    &mut orderbook_get_requests,
                ).await {
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

        if let Some(ExchangeMessage::Orderbook(instrument_id, _stream_name, orderbook)) =
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
        if let Some(ExchangeMessage::Orderbook(instrument_id, _stream_name, orderbook)) =
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
