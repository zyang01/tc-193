use log::info;
use orderbook::orderbook_aggregator_client::OrderbookAggregatorClient;
use orderbook::Empty;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_micros().init();

    let mut client = OrderbookAggregatorClient::connect("http://[::1]:50051")
        .await
        .unwrap();

    let mut stream = client.book_summary(Empty {}).await.unwrap().into_inner();
    while let Some(summary) = stream.message().await.unwrap() {
        info!("{summary:#?}");
    }
}
