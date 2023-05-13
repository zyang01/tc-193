use log::info;
use tc_193::exchange::{bitstamp, ExchangeConnection};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_micros().init();

    info!("Server started");
    let mut conn = bitstamp::BitstampConnection::new();
    conn.subscribe_orderbook("btcusd");

    let (_tx, mut rx) = mpsc::unbounded_channel::<()>();
    rx.recv().await;
}
