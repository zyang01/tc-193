use futures_util::Stream;
use itertools::Itertools;
use log::{info, warn};
use std::collections::HashMap;
use std::pin::Pin;
use tc_193::exchange::{binance, bitstamp, ExchangeConnection, ExchangeMessage};
use tokio::sync::broadcast::{self, Sender};
use tokio::sync::mpsc;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Request, Response, Status};

use orderbook::orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer};
use orderbook::{Empty, Level, Summary};

pub mod orderbook {
    tonic::include_proto!("orderbook");
}

const LEVELS_TO_TAKE: usize = 10;
const BROADCAST_CHANNEL_BUFFER_SIZE: usize = 100;

#[derive(Debug)]
pub struct GrpcServer {
    broadcast_tx: broadcast::Sender<Summary>,
    _broadcast_rx: broadcast::Receiver<Summary>,
}

impl GrpcServer {
    pub fn new() -> Self {
        // create and store broadcast channel
        let (tx, rx) = broadcast::channel(BROADCAST_CHANNEL_BUFFER_SIZE);
        Self {
            broadcast_tx: tx,
            _broadcast_rx: rx,
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for GrpcServer {
    type BookSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

    async fn book_summary(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        info!(
            "Received book summary request from client {:?}",
            request.remote_addr()
        );
        let broadcast_rx = self.broadcast_tx.subscribe();
        let broadcast_stream =
            BroadcastStream::new(broadcast_rx).filter_map(|message| match message {
                Ok(summary) => Some(Ok(summary)),
                Err(BroadcastStreamRecvError::Lagged(_)) => {
                    // warn and skip lagged messages
                    warn!("Broadcast stream lagged");
                    None
                }
            });
        Ok(Response::new(Box::pin(broadcast_stream)))
    }
}

/// Starts a websocket connection to each exchange and broadcasts the best bid and ask for each
/// instrument id
///
/// # Arguments
/// * `broadcast_tx` - Channel to broadcast summaries to
/// * `instrument_ids` - Instrument ids to subscribe to
///
/// # Panics
/// Panics if broadcast_tx is closed or if any of the price or amount fields in the orderbook
/// updates cannot be parsed as floats
async fn book_summary_broadcaster(broadcast_tx: Sender<Summary>, instrument_ids: Vec<String>) {
    // setup websocket connections to exchanges
    let (exchange_message_tx, mut exchange_message_rx) =
        mpsc::unbounded_channel::<ExchangeMessage>();

    let mut bitstamp_conn = bitstamp::BitstampConnection::new(exchange_message_tx.clone());
    let mut binance_conn = binance::BinanceConnection::new(exchange_message_tx);

    for instrument_id in instrument_ids.iter() {
        bitstamp_conn.subscribe_orderbook(instrument_id);
        binance_conn.subscribe_orderbook(instrument_id);
    }

    // Orderbooks by instrument id and exchange name, with values being a tuple of (bids, asks)
    let mut orderbooks: HashMap<String, HashMap<String, (Vec<Level>, Vec<Level>)>> = HashMap::new();

    while let Some(exchange_message) = exchange_message_rx.recv().await {
        let ExchangeMessage::Orderbook(exchange_name, instrument_id, orderbook) = exchange_message;
        let instrument_orderbooks = orderbooks
            .entry(instrument_id.clone())
            .or_insert_with(HashMap::new);

        // convert and store orderbook levels
        instrument_orderbooks.insert(
            exchange_name.clone(),
            (
                orderbook
                    .bids
                    .iter()
                    .map(|level| Level {
                        exchange: exchange_name.clone(),
                        price: level[0].parse().unwrap(),
                        amount: level[1].parse().unwrap(),
                    })
                    .collect_vec(),
                orderbook
                    .asks
                    .iter()
                    .map(|level| Level {
                        exchange: exchange_name.clone(),
                        price: level[0].parse().unwrap(),
                        amount: level[1].parse().unwrap(),
                    })
                    .collect_vec(),
            ),
        );

        // merge orderbook best bids and asks
        let bids = instrument_orderbooks
            .iter()
            .map(|(_, (bids, _))| bids.clone())
            .kmerge_by(|a, b| a.price > b.price || (a.price == b.price && a.amount >= b.amount))
            .take(LEVELS_TO_TAKE)
            .collect_vec();

        let asks = instrument_orderbooks
            .iter()
            .map(|(_, (_, asks))| asks.clone())
            .kmerge_by(|a, b| a.price < b.price || (a.price == b.price && a.amount >= b.amount))
            .take(LEVELS_TO_TAKE)
            .collect_vec();

        // broadcast summary only if there is a best bid and ask
        if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
            broadcast_tx
                .send(Summary {
                    spread: best_ask.price - best_bid.price,
                    bids,
                    asks,
                })
                .unwrap();
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_micros().init();

    let server = GrpcServer::new();
    info!("Server started");

    let instrument_ids = std::env::args().skip(1).collect_vec();
    tokio::spawn(book_summary_broadcaster(
        server.broadcast_tx.clone(),
        instrument_ids,
    ));

    Server::builder()
        .add_service(OrderbookAggregatorServer::new(server))
        .serve("[::1]:50051".parse().unwrap())
        .await
        .unwrap()
}
