use std::collections::{BTreeMap, HashSet};
use super::candle::Candle;
use super::{Time, Price, Volume, Scale};
use super::Bar;
use dtf::Update;
use utils::fill_digits;

/// interval during which some fixed number of volume occurred
type Epoch = u64;

#[derive(Clone, Debug, PartialEq)]
/// utilities for rebinning candlesticks
pub struct VolumeBars {
    pub v: BTreeMap<Epoch, Candle>,
}

impl Bar for VolumeBars {
    fn to_csv(&self) -> String {
        let csvs: Vec<String> = self.v
            .iter()
            .map(|(key, candle)| format!("{},{}", key, candle.to_csv()))
            .collect();

        csvs.join("\n")
    }
}

impl VolumeBars {

    /// Generate a vector of candles sampled by volume traded.
    /// let volume interval be 1,000 shares traded, then each candle
    /// is built from the trade updates that occurred during the interval
    /// in which 1k shares are traded.
    pub fn from_updates(ups: &[Update], vol_interval: f32) -> VolumeBars {

        let mut vol_acc = 0.; // accumulator for traded volume
        let mut epoch = 0;

        let mut candles: BTreeMap<Epoch, Candle> = BTreeMap::new();

        for trade in ups.iter() {
            if !trade.is_trade {
                continue;
            }

            vol_acc += trade.size;
            if vol_acc > vol_interval {
                vol_acc = 0.;
                epoch += 1;
                continue;
            }

            let new_candle = if candles.contains_key(&epoch) {
                let c = candles.get(&epoch).unwrap();
                Candle {
                    volume: c.volume + trade.size,
                    high: if trade.price >= c.high {
                        trade.price
                    } else {
                        c.high
                    },
                    low: if trade.price <= c.low {
                        trade.price
                    } else {
                        c.low
                    },
                    close: trade.price,
                    open: c.open,
                }
            } else {
                Candle {
                    volume: trade.size,
                    high: trade.price,
                    low: trade.price,
                    close: trade.price,
                    open: trade.price,
                }
            };

            candles.insert(epoch, new_candle);
        }

        VolumeBars {
            v: candles,
        }
    }
}

// ... TODO: add tests
