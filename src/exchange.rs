pub mod binance;
pub mod bitstamp;

trait ExchangeConnection {
    fn subscribe_orderbook(&mut self, symbol: &str);
}
