//! Level is the transpose of updates
//! normally updates are of shape [time -> price -> size]
//! this is [price -> time -> size] to keep track of
//! size changes on each price level over time.
use std::collections::{ BTreeMap, HashMap };
use super::utils::price_histogram::{Histogram, Count};


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

    fn from(ups: &[super::Update], step_bins: Count, tick_bins: Count) -> Levels {
        let prices = ups.iter().map(|up| up.price as f64).collect::<Vec<f64>>();
        let price_hist = Histogram::new(&prices, tick_bins);

        let min_ts = ups.iter().next().unwrap().ts / 1000;
        let max_ts = ups.iter().next_back().unwrap().ts / 1000;

        println!("min {} ::::: max {}", min_ts, max_ts);

        let range = (min_ts..max_ts).map(|i| i as f64)
                                    .collect::<Vec<f64>>();
        let time_hist = Histogram::new(range.as_slice(), step_bins);
        println!("{:?}", time_hist.boundaries);

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
        let records = super::super::decode(FNAME, Some(100000));
        let prices = Levels::from(records.as_slice(), 10, 10);
    }
}

// def to_updates(events):
//     tick_bins_cnt = 2000
//     step_bins_cnt = 2000
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
//     return updatesrecords.as_slice