use super::{TickBars, Candle};

const SYMBOL_STICK: &str = "│";
const SYMBOL_CANDLE: &str = "┃";
const SYMBOL_HALF_TOP: &str = "╽";
const SYMBOL_HALF_BOTTOM: &str = "╿";
const SYMBOL_HALF_CANDLE_TOP: &str = "╻";
const SYMBOL_HALF_CANDLE_BOTTOM: &str = "╹";
const SYMBOL_HALF_STICK_TOP: &str = "╷";
const SYMBOL_HALF_STICK_BOTTOM: &str = "╵";
const SYMBOL_NOTHING: &str = " ";

pub struct CandleStickGraph {
    height: u32,
    data: TickBars,
    global_min: f32,
    global_max: f32,
}

impl CandleStickGraph {
    pub fn new(height: u32, data: TickBars) -> Self {
        let global_min = data.v.values()
            .map(|candle| candle.low)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let global_max = data.v.values()
            .map(|candle| candle.high)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        CandleStickGraph {
            height,
            data,
            global_min, 
            global_max,
        }
    }

    pub fn draw(&self) -> String {
        let mut ret = String::new();

        for y in (0..self.height).rev() {
            if y % 4 == 0 {
                ret += &format!("{:8.8} ",
                    self.global_min + 
                    (y as f32 * (self.global_max - self.global_min) 
                        / self.height as f32))
            } else {
                ret += "           "
            }

            for c in self.data.v.values() {
                ret += &self.render_candle_at(c, y);
            }
            ret += "\n"
        }

        ret
    }

    fn to_height_units(&self, x: f32) -> f32 {
        (x - self.global_min) / (self.global_max - self.global_min)
            * self.height as f32
    }


    fn render_candle_at(&self, candle: &Candle, height_unit: u32) -> String {
        let height_unit = height_unit as f32;

        let ts = self.to_height_units(candle.high);
        let tc = self.to_height_units(candle.open.max(candle.close));

        let bs = self.to_height_units(candle.low);
        let bc = self.to_height_units(candle.open.min(candle.close));

        if f32::ceil(ts) >= height_unit && height_unit >= f32::floor(tc) {
            if tc - height_unit > 0.75 {
                return SYMBOL_CANDLE.to_owned()
            } else if (tc - height_unit) > 0.25 {
                if (ts - height_unit) > 0.75 {
                    return SYMBOL_HALF_TOP.to_owned()
                } else {
                    return SYMBOL_HALF_CANDLE_TOP.to_owned()
                }
            } else {
                if (ts - height_unit) > 0.75 {
                    return SYMBOL_STICK.into()
                } else if  (ts - height_unit) > 0.25 {
                    return SYMBOL_HALF_STICK_TOP.into()
                } else {
                    return SYMBOL_NOTHING.into()
                }
            }
        } else if f32::floor(tc) >= height_unit && height_unit >= f32::ceil(bc) {
            return SYMBOL_CANDLE.to_owned()
        } else if f32::ceil(bc) >= height_unit && height_unit >= f32::floor(bs) {
            if (bc - height_unit) < 0.25 {
                return SYMBOL_CANDLE.to_owned()
            } else if (bc - height_unit) < 0.75 {
                if (bs - height_unit) < 0.25 {
                    return SYMBOL_HALF_BOTTOM.to_owned()
                } else {
                    return SYMBOL_HALF_CANDLE_BOTTOM.to_owned()
                }
            } else {
                if (bs - height_unit) < 0.25 {
                    return SYMBOL_STICK.into()
                } else if (bs - height_unit) < 0.75 {
                    return SYMBOL_HALF_STICK_BOTTOM.into()
                } else {
                    return SYMBOL_NOTHING.into()
                }
            }
        } else {
            return SYMBOL_NOTHING.into()
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn should_print_candlestick_graph_ok() {
    //     static HOUR : u64 = 60 * 60 * 1000 - 1000; 
    //     static MINUTE : u64 = 60 * 1000; 

    //     let fname: &str = "./bt_btcneo.dtf";
    //     let meta = dtf::read_meta(fname).unwrap();

    //     let min_ts = meta.min_ts + HOUR;
    //     let y_ts = 10 * MINUTE;
    //     let max_ts = min_ts + HOUR + y_ts;

    //     let ups = dtf::get_range_in_file(fname, min_ts, max_ts).unwrap();
    //     let mut candles = Candles::from(ups.as_slice());
    //     candles.insert_continuation_candles();
    //     let graph = CandleStickGraph::new(21, candles);
    // }
}