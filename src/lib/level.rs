//! Level is the transpose of updates
//! normally updates are of shape [time -> price -> size]
//! this is [price -> time -> size] to keep track of
//! size changes on each price level over time.
use std::collections::{ BTreeMap, HashMap };
use super::utils::price_histogram::PriceHistogram;

type Price = f32;
type Size = f32;
type Time = u32;

struct Levels {
    levels: HashMap<Price, BTreeMap<Time, Size>>
}

impl Levels {
    fn new() -> Levels {
        unimplemented!()
    }
}

impl<'a> From<&'a [super::Update]> for Levels {
    fn from(ups: &[super::Update]) -> Levels {
        // let prices = ups.iter().map(|up| up.price as f64).collect();
        // let mut hist = PriceHistogram::new(&prices);
        // println!("{:?}", hist.get_percentile(50));

        unimplemented!();

        // Levels {
        //     levels
        // }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    static FNAME : &str = "test-data/bt_btcnav.dtf";

    #[test]
    pub fn test_levels() {
        let records = super::super::decode(FNAME, Some(10000));
        let prices = Levels::from(records.as_slice());

    }
}

// def to_updates(events):
//     tick_bins_cnt = 2000
//     step_bins_cnt = 2000
//     def into_tick_bin(price):
//         for (s, b) in zip(boundaries, boundaries[1:]):
//             if b > price > s:
//                 return s
//         return False
//     min_ts = result[0][0]
//     max_ts = result[-1][0]
//     step_thresholds = range(int(floor(min_ts)), int(ceil(max_ts)), int(floor((max_ts - min_ts)/(step_bins_cnt))))
//     def into_step_bin(time):
//         for (s, b) in zip(step_thresholds, step_thresholds[1:]):
//             if b > time > s:
//                 return b
//         return False
//     updates = {}
//     for row in result:
//         ts, seq, size, price, is_bid, is_trade = row
//         price = into_tick_bin(price)
//         time = into_step_bin(ts)
//         if not price or not time:
//             continue
//         if price not in updates:
//             updates[price] = {}
//         if time not in updates[price]:
//             updates[price][time] = 0
//         updates[price][time] += size;
//     return updates