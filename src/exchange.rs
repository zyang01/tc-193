use tokio::sync::mpsc;

pub mod binance;
pub mod bitstamp;

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

#[derive(Debug)]
pub struct Orderbook {
    /// Monotonic counter
    monotonic_counter: u64,

    /// Microtimestamp of the orderbook
    microtimestamp: Option<u64>,

    /// Bids represented as `[[price, amount], ...]`
    bids: Vec<[String; 2]>,

    /// Asks represented as `[[price, amount], ...]`
    asks: Vec<[String; 2]>,
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
