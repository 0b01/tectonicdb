//! Level is the transpose of updates
//! normally updates are of shape [time -> price -> size]
//! this is [price -> time -> size] to keep track of
//! size changes on each price level over time.
use std::collections::{BTreeMap, HashMap};
use crate::postprocessing::histogram::{Histogram, BinCount};
use crate::utils::fill_digits;
use crate::dtf::update::Update;

type Price = u64;
type Time = u32;
type Size = f32;

/// data structure for storing levels
#[derive(Debug)]
pub struct Levels {
    levels: HashMap<Price, BTreeMap<Time, Size>>,
}

impl Levels {
    /// converts a slice of Update to [price, time, size]
    /// see how price levels evolve over time...
    pub fn from(ups: &[Update], step_bins: BinCount, tick_bins: BinCount, m: f64) -> Levels {
        let (price_hist, step_hist) = Histogram::from(&ups, step_bins, tick_bins, m);
        println!("{:?}", step_hist);

        // build map for levels
        let mut map = HashMap::new();
        for up in ups.iter() {
            let price = price_hist.to_bin(up.price as f64);
            let time = step_hist.to_bin((fill_digits(up.ts) / 1000) as f64);
            match (price, time) {
                (Some(p), Some(t)) => {
                    let price_level = map.entry(p.to_bits()).or_insert(
                        BTreeMap::<Time, Size>::new(),
                    );
                    (*price_level).insert(t as Time, up.size);
                }
                (None, _) => {
                    continue;
                }
                (_, None) => {
                    continue;
                }
            }
        }

        Levels { levels: map }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtf;
    static FNAME: &str = "../../test/test-data/bt_btcnav.dtf";

    #[test]
    pub fn test_levels() {
        // rebin price
        let tick_bins = 10; // or 9 thresholds
        let step_bins = 10;
        let records = dtf::file_format::decode(FNAME, Some(100)).unwrap();
        {
            let prices = records
                .iter()
                .map(|up| up.price as f64)
                .collect::<Vec<f64>>();
            let price_hist = Histogram::new(&prices, tick_bins, 2.0);
            let mut dict = BTreeMap::new();
            for up in records.iter() {
                if let Some(binned_val) = price_hist.to_bin(up.price as f64) {
                    let entry = dict.entry(binned_val.to_bits()).or_insert(0);
                    (*entry) += 1;
                }
            }
            assert_eq!(price_hist.boundaries.len(), tick_bins);
            assert_eq!(price_hist.bins.clone().unwrap().len(), tick_bins);

            for (val, bin) in dict.values().zip(price_hist.bins.unwrap().iter()) {
                assert_eq!(val, bin);
            }
        }

        let levels = Levels::from(records.as_slice(), step_bins, tick_bins, 2.);
        assert_eq!(
            levels.levels.keys().collect::<Vec<_>>().len(),
            tick_bins - 1
        );
        for level in levels.levels.values() {
            assert!(level.keys().collect::<Vec<_>>().len() <= (step_bins - 1));
        }

    }
}
