//! Level is the transpose of updates
//! normally updates are of shape [time -> price -> size]
//! this is [price -> time -> size] to keep track of
//! size changes on each price level over time.
use std::collections::{ BTreeMap, HashMap };
use super::utils::price_histogram::{Histogram, Count};


type Price = f32;
type Size = f32;
type Time = u32;
type PriceBits = u64;

struct Levels {
    levels: HashMap<PriceBits, BTreeMap<Time, Size>>
}

impl Levels {
    fn new() -> Levels {
        unimplemented!()
    }

    fn from(ups: &[super::Update], step_bins: Count, tick_bins: Count) -> Levels {
        // build price histogram
        let prices = ups.iter().map(|up| up.price as f64).collect::<Vec<f64>>();
        let price_hist = Histogram::new(&prices, tick_bins);

        // build time step histogram
        let min_ts = ups.iter().next().unwrap().ts / 1000;
        let max_ts = ups.iter().next_back().unwrap().ts / 1000;
        let bucket_size = (max_ts - min_ts) / (step_bins as u64);
        let mut boundaries = vec![];
        for i in 0..(step_bins+1) {
            boundaries.push((min_ts + i as u64) as f64 * bucket_size as f64);
        }
        let step_hist = Histogram { bins: None, boundaries };

        // build map for levels
        let mut map = HashMap::new();
        for up in ups.iter() {
            let price = price_hist.to_bin(up.price as f64);
            let time = step_hist.to_bin((up.ts / 1000) as f64);
            match (price, time) {
                (None, _) => { continue; },
                (_, None) => { continue; },
                (Some(p), Some(t)) => {
                    let price_level = map.entry(p.to_bits()).or_insert(BTreeMap::<Time, Size>::new());
                    (*price_level).insert(t as Time, up.size);
                }
            }
        };

        Levels {
            levels: map
        }
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

// def to_levels(events):
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