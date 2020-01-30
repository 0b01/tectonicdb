use crate::dtf;
/// data structure for candle stick
pub mod candle;
/// candles sampled by time
pub mod time_bars;
/// candles sampled by volume
pub mod volume_bars;
/// plot candlesticks in terminal
pub mod candlestick_graph;

pub use self::time_bars::TimeBars;
pub use self::volume_bars::VolumeBars;
pub use self::candle::Candle;

use self::dtf::update::Update;

type Time = u32;
type Price = f32;
type Volume = f32;
type Scale = u16;

/// draw a list of updates in the terminal
pub fn draw_updates(ups: &[Update]) -> String {
    let mut candles = TimeBars::from(ups);
    candles.insert_continuation_candles();
    candlestick_graph::CandleStickGraph::new(20, candles.clone()).draw()
}

