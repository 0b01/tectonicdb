use super::candle::Candle;
use crate::dtf::update::Update;

type Time = u64;

/// interval during which some fixed number of volume occurred
type Epoch = u64;

/// Iterator for Bars sampled by time, default is 1 minute bar
pub struct VolumeBarsIter<I:Iterator<Item=Update>> {
    it: I,
    epoch: Epoch,
    vol_interval: f32,
    current_candle: Option<((Time, Time), Candle)>,
}

impl<I:Iterator<Item=Update>> VolumeBarsIter<I> {
    /// Create a new iterator for time bars
    pub fn new(it: I, vol_interval: f32) -> Self {
        Self {
            it,
            epoch: 0,
            current_candle: None,
            vol_interval,
        }
    }
}

impl<I:Iterator<Item=Update>> Iterator for VolumeBarsIter<I> {
    type Item = ((Time, Time), Candle);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(trade) = self.it.next() {
            if !trade.is_trade {
                continue;
            }

            if let Some(((t0, tn), c)) = self.current_candle {
                if c.volume >= self.vol_interval {
                    self.current_candle = Some(((trade.ts, trade.ts), Candle {
                        volume: trade.size,
                        high: trade.price,
                        low: trade.price,
                        close: trade.price,
                        open: trade.price,
                    })) ;
                    self.epoch += 1;
                    return Some(((t0, tn), c));
                };
            }

            self.current_candle = Some(if let Some(((t0, _tn), c)) = self.current_candle {
                ((t0, trade.ts), Candle {
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
                })
            } else {
                ((trade.ts, trade.ts), Candle {
                    volume: trade.size,
                    high: trade.price,
                    low: trade.price,
                    close: trade.price,
                    open: trade.price,
                })
            });

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
    v: Vec<((Time, Time), Candle)>,
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

        assert_eq!(VolumeBars {v: vec![((0,8), Candle {
                open: 0.0,
                high: 8.0,
                low: 0.0,
                close: 8.0,
                volume: 36.0,
            }), ((9,9), Candle {
                open: 9.0,
                high: 9.0,
                low: 9.0,
                close: 9.0,
                volume: 9.0,
            })]}, ret);
    }
}
