pub mod binance;
pub mod bitstamp;

pub trait ExchangeConnection {
    fn subscribe_orderbook(&mut self, symbol: &str);

    fn new() -> Self;
}
