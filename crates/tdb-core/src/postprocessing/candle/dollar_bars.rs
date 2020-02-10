use super::{Candle, Sample};
use crate::dtf::update::Update;

/// sample by dollar traded
pub struct DollarSampler {
    interval: f32,
    elapsed: f32,
}

impl DollarSampler {
    /// create a new Dollar sampler
    pub fn new(interval: f32) -> Self {
        Self {
            elapsed: 0.,
            interval,
        }
    }
}

impl Sample for DollarSampler {
    fn is_sample(&mut self, trade: &Update) -> bool {
        self.elapsed += trade.price * trade.size;

        if self.elapsed > self.interval {
            self.elapsed = 0.;
            true
        } else {
            false
        }
    }
}

/// Iterator for Bars sampled by dollars traded
pub struct DollarBarsIter<I:Iterator<Item=Update>> {
    it: I,
    current_candle: Option<Candle>,
    sampler: DollarSampler,
}

impl<I:Iterator<Item=Update>> DollarBarsIter<I> {
    /// Create a new iterator for time bars
    pub fn new(it: I, dollar_interval: f32) -> Self {
        Self {
            it,
            current_candle: None,
            sampler: DollarSampler::new(dollar_interval),
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

impl<I:Iterator<Item=Update>> Iterator for DollarBarsIter<I> {
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
pub struct DollarBars {
    v: Vec<Candle>,
}

impl DollarBars {

    /// Generate a vector of candles sampled by dollar traded.
    pub fn from_updates(ups: &[Update], dollar_interval: f32) -> DollarBars {
        let v = DollarBarsIter::new(ups.iter().copied(), dollar_interval).collect();
        DollarBars { v }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32;
    #[test]
    fn test_dollar_bar() {
        let trades = (0..10).map(|i| Update {
            is_trade: true,
            is_bid: true,
            price: i as f32,
            size: f32::abs(i as f32),
            ts: i,
            seq: 0,
        })
        .collect::<Vec<_>>();

        let ret = DollarBars::from_updates(&trades, 100.);

        assert_eq!(DollarBars {v: vec![Candle {
                start: 0,
                end: 6,
                open: 0.0,
                high: 6.0,
                low: 0.0,
                close: 6.0,
                volume: 21.0,
            }, Candle {
                start: 7,
                end: 8,
                open: 7.0,
                high: 8.0,
                low: 7.0,
                close: 8.0,
                volume: 15.0,
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
