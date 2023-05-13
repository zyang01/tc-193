use super::ExchangeConnection;
use log::info;

pub struct BinanceConnection {
    subscribed_channels: Vec<String>,
}

impl ExchangeConnection for BinanceConnection {
    fn subscribe_orderbook(&mut self, symbol: &str) {
        info!("Subscribing to {} on Binance", symbol);
        self.subscribed_channels.push(symbol.to_string());
    }
    fn new() -> Self {
        Self {
            subscribed_channels: vec![],
        }
    }
}
