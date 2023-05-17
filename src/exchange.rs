pub mod binance;
pub mod bitstamp;

#[derive(Debug)]
enum Command {
    SubscribeOrderbook(String),
}

#[derive(Debug)]
struct Orderbook {
    microtimestamp: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

pub trait ExchangeConnection {
    fn subscribe_orderbook(&mut self, symbol: &str);

    fn new() -> Self;
}
