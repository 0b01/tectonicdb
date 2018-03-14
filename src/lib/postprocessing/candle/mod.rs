use dtf;

pub mod candlestick_graph;
pub mod candles;
pub mod candle;

pub use self::candles::Candles;
pub use self::candle::Candle;

type Time = u32;
type Price = f32;
type Volume = f32;
type Scale = u16;

pub fn draw_updates(ups: &[dtf::Update]) -> String {
    let candles = Candles::from(ups);
    candlestick_graph::CandleStickGraph::new(20, candles.clone()).draw()
}