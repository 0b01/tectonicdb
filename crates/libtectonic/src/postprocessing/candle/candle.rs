use super::{Price, Volume, Time};

#[derive(PartialOrd, PartialEq, Clone, Copy, Debug)]
/// a candlestick
pub struct Candle {
    /// start ts
    pub start: Time,
    /// end ts
    pub end: Time,
    /// open price
    pub open: Price,
    /// high price
    pub high: Price,
    /// low price
    pub low: Price,
    /// close price
    pub close: Price,
    /// volume
    pub volume: Volume,
}

impl Eq for Candle {}

impl Candle {
    /// convert to csv
    /// Format:
    ///     S,E,O,H,L,C,V
    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},{},{},{},{}",
            self.start,
            self.end,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume
        )
    }
}
