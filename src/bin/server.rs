use futures_util::Stream;
use log::{info, warn};
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{transport::Server, Request, Response, Status};

use orderbook::orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer};
use orderbook::{Empty, Level, Summary};

pub mod orderbook {
    tonic::include_proto!("orderbook");
}

const BROADCAST_CHANNEL_BUFFER_SIZE: usize = 100;

#[derive(Debug)]
pub struct GrpcServer {
    broadcast_tx: broadcast::Sender<Summary>,
    _broadcast_rx: broadcast::Receiver<Summary>,
}

impl GrpcServer {
    pub fn new() -> Self {
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
                    warn!("Broadcast stream lagged");
                    None
                }
            });
        Ok(Response::new(Box::pin(broadcast_stream)))
    }
}

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_micros().init();
    info!("Server started");

    let server = GrpcServer::new();

    let broadcast_tx = server.broadcast_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let summary = Summary {
                spread: 1.0,
                asks: vec![Level {
                    exchange: "exchange".to_string(),
                    price: 1.0,
                    amount: 1.0,
                }],
                bids: vec![],
            };
            broadcast_tx.send(summary).unwrap();
        }
    });

    Server::builder()
        .add_service(OrderbookAggregatorServer::new(server))
        .serve("[::1]:50051".parse().unwrap())
        .await
        .unwrap()
}
