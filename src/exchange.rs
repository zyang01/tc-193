use tokio::sync::mpsc;

pub mod binance;
pub mod bitstamp;

#[derive(Debug)]
enum Command {
    SubscribeOrderbook(String),
}

#[derive(Debug)]
pub enum ExchangeMessage {
    /// Represent an orderbook as `(exchange_name, instrument_id, orderbook)`
    Orderbook(String, String, Orderbook),
}

#[derive(Debug)]
pub struct Orderbook {
    monotonic_counter: u64,
    microtimestamp: Option<u64>,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

pub trait ExchangeConnection {
    fn subscribe_orderbook(&mut self, instrument_id: &str);

    fn new(exchange_message_tx: mpsc::UnboundedSender<ExchangeMessage>) -> Self;
}
