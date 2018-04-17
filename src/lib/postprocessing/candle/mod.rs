use dtf;

pub mod candlestick_graph;
pub mod tick_bar;
pub mod volume_bar;
pub mod candle;

pub use self::tick_bar::TickBars;
pub use self::candle::Candle;

type Time = u32;
type Price = f32;
type Volume = f32;
type Scale = u16;

pub fn draw_updates(ups: &[dtf::Update]) -> String {
    let mut candles = TickBars::from(ups);
    candles.insert_continuation_candles();
    candlestick_graph::CandleStickGraph::new(20, candles.clone()).draw()
}


pub trait Bar {
    /// convert TickBars vector to csv
    /// format is
    ///     T,O,H,L,C,V
    fn to_csv(&self) -> String;
}