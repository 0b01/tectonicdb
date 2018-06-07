use super::{Price, Volume};

#[derive(PartialOrd, PartialEq, Clone, Debug)]
/// a candlestick
pub struct Candle {
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Volume,
}

impl Eq for Candle {}

impl Candle {
    /// convert to csv
    /// Format:
    ///     O,H,L,C,V
    pub fn to_csv(&self) -> String {
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
