use log::info;
use tc_193::exchange::{binance, bitstamp, ExchangeConnection, ExchangeMessage};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_micros().init();

    info!("Server started");

    let (exchange_message_tx, mut exchange_message_rx) =
        mpsc::unbounded_channel::<ExchangeMessage>();

    let mut conn = bitstamp::BitstampConnection::new(exchange_message_tx.clone());
    conn.subscribe_orderbook("btcusd");

    let mut binance_conn = binance::BinanceConnection::new(exchange_message_tx);
    binance_conn.subscribe_orderbook("btcusdt");

    while let Some(exchange_message) = exchange_message_rx.recv().await {
        info!("Received message: {:?}", exchange_message);
    }
}
