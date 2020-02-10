use super::{Candle, Sampler};
use crate::dtf::update::Update;

/// sample by fixed number of ticks
pub struct TickSampler {
    interval: u32,
    elapsed: u32,
}

impl TickSampler {
    /// create a new tick sampler
    pub fn new(interval: u32) -> Self {
        Self {
            elapsed: 0,
            interval,
        }
    }
}

impl Sampler for TickSampler {
    fn reset(&mut self) {
        self.elapsed = 0;
    }
    fn is_sample(&mut self, _update: &Update) -> bool {
        self.elapsed += 1;

        if self.elapsed == self.interval + 1 {
            self.elapsed = 1;
            true
        } else {
            false
        }
    }
}

/// Iterator for Bars sampled by fixed number of tick
pub struct TickBarsIter<I:Iterator<Item=Update>> {
    it: I,
    current_candle: Option<Candle>,
    sampler: TickSampler,
}

impl<I:Iterator<Item=Update>> TickBarsIter<I> {
    /// Create a new iterator for time bars
    pub fn new(it: I, tick_interval: u32) -> Self {
        Self {
            it,
            current_candle: None,
            sampler: TickSampler::new(tick_interval),
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

impl<I:Iterator<Item=Update>> Iterator for TickBarsIter<I> {
    type Item = Candle;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(trade) = self.it.next() {
            let is_sample = self.sampler.is_sample(&trade);
            if !trade.is_trade {
                continue;
            }

            if let Some(c) = self.current_candle {
                if is_sample {
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
pub struct TickBars {
    v: Vec<Candle>,
}

impl TickBars {
    /// Generate a vector of candles sampled by ticks.
    pub fn from_updates(ups: &[Update], tick_interval: u32) -> TickBars {
        let v = TickBarsIter::new(ups.iter().copied(), tick_interval).collect();
        TickBars { v }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32;
    #[test]
    fn test_tick_bar() {
        let trades = (0..10).map(|i| Update {
            is_trade: true,
            is_bid: true,
            price: i as f32,
            size: f32::abs(i as f32),
            ts: i,
            seq: 0,
        })
        .collect::<Vec<_>>();

        let ret = TickBars::from_updates(&trades, 3);

        assert_eq!(TickBars {v: vec![Candle {
                start: 0,
                end: 2,
                open: 0.0,
                high: 2.0,
                low: 0.0,
                close: 2.0,
                volume: 3.0,
            }, Candle {
                start: 3,
                end: 5,
                open: 3.0,
                high: 5.0,
                low: 3.0,
                close: 5.0,
                volume: 12.0,
            }, Candle {
                start: 6,
                end: 8,
                open: 6.0,
                high: 8.0,
                low: 6.0,
                close: 8.0,
                volume: 21.0,
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
