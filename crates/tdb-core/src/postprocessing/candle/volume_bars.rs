use super::{Candle, Sample};
use crate::dtf::update::Update;

/// sample by volume traded
pub struct VolumeSampler {
    interval: f32,
    elapsed: f32,
}

impl VolumeSampler {
    /// create a new Volume sampler
    pub fn new(interval: f32) -> Self {
        Self {
            elapsed: 0.,
            interval,
        }
    }
}

impl Sample for VolumeSampler {
    fn is_sample(&mut self, trade: &Update) -> bool {
        self.elapsed += trade.size;

        if self.elapsed > self.interval {
            self.elapsed = 0.;
            true
        } else {
            false
        }
    }
}

/// Iterator for Bars sampled by volume
pub struct VolumeBarsIter<I:Iterator<Item=Update>> {
    it: I,
    current_candle: Option<Candle>,
    sampler: VolumeSampler,
}

impl<I:Iterator<Item=Update>> VolumeBarsIter<I> {
    /// Create a new iterator for time bars
    pub fn new(it: I, vol_interval: f32) -> Self {
        Self {
            it,
            current_candle: None,
            sampler: VolumeSampler::new(vol_interval),
        }
    }
}

fn new_candle(trade: Update) -> Candle {
    Candle {
        start: trade.ts,
        end: trade.ts,
        volume: trade.size,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        open: trade.price,
    }
}

impl<I:Iterator<Item=Update>> Iterator for VolumeBarsIter<I> {
    type Item = Candle;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(trade) = self.it.next() {
            if !trade.is_trade {
                continue;
            }

            if let Some(c) = self.current_candle {
                if self.sampler.is_sample(&trade) {
                    self.current_candle = Some(new_candle(trade));
                    return Some(c);
                };
            }

            self.current_candle = if let Some(c) = self.current_candle {
                Some(Candle {
                    start: c.start,
                    end: trade.ts,
                    volume: c.volume + trade.size,
                    high: trade.price.max(c.high),
                    low: trade.price.min(c.low),
                    close: trade.price,
                    open: c.open,
                })
            } else {
                Some(new_candle(trade))
            };

        }
        if let Some(x) = self.current_candle {
            self.current_candle = None;
            return Some(x)
        } else {
            None
        }
    }
}


#[derive(Clone, Debug, PartialEq)]
/// utilities for rebinning candlesticks
pub struct VolumeBars {
    v: Vec<Candle>,
}

impl VolumeBars {

    /// Generate a vector of candles sampled by volume traded.
    /// let volume interval be 1,000 shares traded, then each candle
    /// is built from the trade updates that occurred during the interval
    /// in which 1k shares are traded.
    pub fn from_updates(ups: &[Update], vol_interval: f32) -> VolumeBars {
        let v = VolumeBarsIter::new(ups.iter().copied(), vol_interval).collect();
        VolumeBars { v }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32;
    #[test]
    fn test_vol_bar() {
        let trades = (0..10).map(|i| Update {
            is_trade: true,
            is_bid: true,
            price: i as f32,
            size: f32::abs(i as f32),
            ts: i,
            seq: 0,
        })
        .collect::<Vec<_>>();

        let ret = VolumeBars::from_updates(&trades, 36.);

        assert_eq!(VolumeBars {v: vec![Candle {
                start: 0,
                end: 8,
                open: 0.0,
                high: 8.0,
                low: 0.0,
                close: 8.0,
                volume: 36.0,
            }, Candle {
                start: 9,
                end: 9,
                open: 9.0,
                high: 9.0,
                low: 9.0,
                close: 9.0,
                volume: 9.0,
            }]}, ret);
    }
}
