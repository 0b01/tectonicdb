use super::{Price, Volume};

#[derive(PartialOrd, PartialEq, Clone, Debug)]
/// a candlestick
pub struct Candle {
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
    ///     O,H,L,C,V
    pub fn as_csv(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume
        )
    }
}
