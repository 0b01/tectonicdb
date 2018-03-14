pub mod candlestick_graph;
pub mod candles;
pub mod candle;

pub use self::candles::Candles;
pub use self::candle::Candle;

use std::collections::{HashSet};


type Time = u32;
type Price = f32;
type Volume = f32;
type Scale = u16;