use super::ExchangeConnection;
use log::info;

pub struct BitstampConnection {
    subscribed_channels: Vec<String>,
}

impl ExchangeConnection for BitstampConnection {
    fn subscribe_orderbook(&mut self, symbol: &str) {
        info!("Subscribing to {} on Bitstamp", symbol);
        self.subscribed_channels.push(symbol.to_string());
    }
}

impl BitstampConnection {
    pub fn new() -> Self {
        Self {
            subscribed_channels: vec![],
        }
    }
}
