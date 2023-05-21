use tokio::sync::mpsc;

pub mod binance;
pub mod bitstamp;

#[derive(Debug)]
pub struct Orderbook {
    /// Monotonic counter
    monotonic_counter: u64,

    /// Bids represented as `[[price, amount], ...]`
    pub bids: Vec<[String; 2]>,

    /// Asks represented as `[[price, amount], ...]`
    pub asks: Vec<[String; 2]>,
}

impl Orderbook {
    /// Returns true if the orderbook is newer than the other orderbook
    pub fn is_newer_than(&self, other: &Orderbook) -> bool {
        self.monotonic_counter > other.monotonic_counter
    }
}

/// Exchange connection commands
#[derive(Debug)]
enum Command {
    /// Subscribe to orderbook updates for an instrument id
    SubscribeOrderbook(String),
}

/// Exchange message types
#[derive(Debug)]
pub enum ExchangeMessage {
    /// Represent an orderbook as `(exchange_name, instrument_id, orderbook)`
    Orderbook(String, String, Orderbook),
}

/// Exchange connection trait
pub trait ExchangeConnection {
    /// Subscribe to orderbook updates for an instrument id
    ///
    /// # Arguments
    /// * `instrument_id` - Instrument id to subscribe to
    fn subscribe_orderbook(&mut self, instrument_id: &str);

    /// Create a new exchange connection
    ///
    /// # Arguments
    /// * `exchange_message_tx` - Channel to send exchange messages to
    fn new(exchange_message_tx: mpsc::UnboundedSender<ExchangeMessage>) -> Self;
}
