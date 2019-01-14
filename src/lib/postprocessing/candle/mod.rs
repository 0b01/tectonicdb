use crate::dtf;
/// data structure for candle stick
pub mod candle;
/// data structure for storing candles
pub mod tick_bar;
/// data structure for storing volume candles
pub mod volume_bar;
/// plot candlesticks in terminal
pub mod candlestick_graph;

pub use self::tick_bar::TickBars;
pub use self::volume_bar::VolumeBars;
pub use self::candle::Candle;

use self::dtf::update::Update;

type Time = u32;
type Price = f32;
type Volume = f32;
type Scale = u16;

/// draw a list of updates in the terminal
pub fn draw_updates(ups: &[Update]) -> String {
    let mut candles = TickBars::from(ups);
    candles.insert_continuation_candles();
    candlestick_graph::CandleStickGraph::new(20, candles.clone()).draw()
}

