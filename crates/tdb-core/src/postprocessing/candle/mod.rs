use crate::dtf;

/// candles sampled by time
pub mod time_bars;
/// candles sampled by volume
pub mod volume_bars;
/// candles sampled by fixed number of ticks
pub mod tick_bars;
/// candles sampled by dollar traded
pub mod dollar_bars;
/// plot candlesticks in terminal
pub mod candlestick_graph;
use self::dtf::update::Update;

type Time = u64;
type Price = f32;
type Volume = f32;
type Scale = u16;

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

/// draw a list of updates in the terminal
pub fn draw_updates(ups: &[Update]) -> String {
    let mut candles = time_bars::TimeBars::from(ups);
    candles.insert_continuation_candles();
    candlestick_graph::CandleStickGraph::new(20, candles.clone()).draw()
}

/// determines whether to sample based on update
pub trait Sample {
    /// should a sample be generated after this update
    fn is_sample(&mut self, update: &Update) -> bool;
}
