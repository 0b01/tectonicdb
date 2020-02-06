use std::collections::HashSet;
use super::candle::Candle;
use super::{Time, Scale};
use crate::dtf::update::Update;
use crate::utils::fill_digits;
use indexmap::IndexMap;

/// Iterator for Bars sampled by time, default is 1 minute bar
pub struct TimeBarsIter<I:Iterator<Item=Update>> {
    it: I,
    current_candle: Option<Candle>,
}

impl<I:Iterator<Item=Update>> TimeBarsIter<I> {
    /// Create a new iterator for time bars
    pub fn new(it: I) -> Self {
        Self {
            it,
            current_candle: None,
        }
    }
}

fn new_candle(t: Time, trade: Update) -> Candle {
    Candle {
        start: t,
        end: t,
        volume: trade.size,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        open: trade.price,
    }
}

impl<I:Iterator<Item=Update>> Iterator for TimeBarsIter<I> {
    type Item = Candle;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(trade) = self.it.next() {
            if !trade.is_trade {
                continue;
            }

            let ts = (fill_digits(trade.ts) / 1000 / 60 * 60) as Time; // floor(ts)

            self.current_candle = if let Some(c) = &self.current_candle {
                if c.start != ts {
                    let c = *c;
                    self.current_candle = Some(new_candle(ts, trade));
                    return Some(c);
                } else {
                    Some(Candle {
                        start: ts,
                        end: ts,
                        volume: c.volume + trade.size,
                        high: trade.price.max(c.high),
                        low: trade.price.min(c.low),
                        close: trade.price,
                        open: c.open,
                    })
                }
            } else {
                Some(new_candle(ts, trade))
            };
        }
        None
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Candles sampled by time, allows rebinning
pub struct TimeBars {
    v: IndexMap<Time, Candle>,
    scale: Scale,
}

impl<'a> From<&'a [Update]> for TimeBars {
    /// Generate a vector of 1-min candles from Updates
    fn from(ups: &[Update]) -> TimeBars {
        let candles = TimeBarsIter::new(ups.iter().copied()).map(|c| (c.start, c)).collect();
        return TimeBars::new(candles, 1);
    }
}

impl TimeBars {

    /// Get total length of candles
    pub fn get_size(&self) -> usize {
        self.v.len()
    }

    /// Get tick bars as a slice
    pub fn get_candles<'a>(&'a self) -> indexmap::map::Values<'a, Time, Candle> {
        self.v.values()
    }

    /// return the scale of candles
    pub fn get_scale(&self) -> Scale {
        self.scale
    }

    /// convert TimeBars vector to csv
    /// format is
    ///     T,O,H,L,C,V
    pub fn to_csv(&self) -> String {
        let csvs: Vec<String> = self.v
            .iter()
            .map(|(key, candle)| format!("{},{}", key, candle.to_csv()))
            .collect();

        csvs.join("\n")
    }

    /// Find missing epochs (in minute)
    /// Some epochs may be missing from the candles for some reason.
    /// For example, some instruments may have low volatility for 1 minute, or the
    /// exchange is down, or the collection backend is flooded.
    fn missing_epochs(&self) -> Vec<Time> {

        let mut set = HashSet::<Time>::new();
        let mut missing = Vec::<Time>::new();

        for &ts in self.v.keys() {
            set.insert(ts);
        }

        let &max_epoch = self.v.keys().next_back().unwrap();
        let &min_epoch = self.v.keys().next().unwrap();

        let mut it = min_epoch;
        while it < max_epoch {
            if !set.contains(&it) {
                missing.push(it);
            }
            it += (self.scale as Time) * 60;
        }

        missing
    }

    /// returns the ranges of missing epochs
    /// [60, 120, 280, 360] => [(180, 280)]
    pub fn missing_ranges(&self) -> Vec<(Time, Time)> {
        ranges(&self.missing_epochs())
    }

    /// insert continuation candles and fix missing
    /// insert the missing candles based on the previous candle
    pub fn insert_continuation_candles(&mut self) {
        let (mut last_ts, mut last_close) = {
            let (&last_ts, row) = self.v.iter().next().unwrap(); // first ts here, last ts later
            (last_ts, row.close)
        };

        let mut temp = IndexMap::<Time, Candle>::new();

        for (&ts, row) in self.v.iter() {
            if (ts != last_ts + 60) && (last_ts != 0) && (last_ts != ts) {
                //insert continuation candle(s)
                let mut cur = last_ts + 60;
                while cur < ts {
                    temp.insert(
                        cur,
                        Candle {
                            start: cur,
                            end: cur + 60,
                            volume: 0.,
                            high: last_close,
                            low: last_close,
                            open: last_close,
                            close: last_close,
                        },
                    );
                    cur += 60;
                }
            }
            last_ts = ts;
            last_close = row.close;
        }

        self.v.extend(temp);
    }


    /// create new TimeBars object
    fn new(v: IndexMap<Time, Candle>, scale: u16) -> TimeBars {
        let ret = TimeBars { v, scale };

        ret
    }

    /// epochs must be exactly incrementing by 60
    /// 60, 120, 180 => ok!
    /// 60,    , 180 => not ok!
    fn _test_epochs_must_be_sequential(&self) -> bool {
        // all([a[0] + i * 60 * minutes == x for i, x in enumerate(a)])
        let mut i: Time = 0;
        let &first = self.v.keys().next().unwrap();
        for &row in self.v.keys() {
            if first + i * 60 * (self.scale as Time) != row {
                return false;
            }
            i += 1;
        }
        true
    }



    /// rebin 1 minute candles to candles sampled by new scale
    pub fn rebin(self, align: bool, new_scale: u16) -> Option<TimeBars> {
        if new_scale < self.scale {
            return None;
        } else if new_scale == self.scale {
            return Some(self);
        }

        let mut res = IndexMap::new();

        let mut startacc = 0;
        let mut openacc = 0.;
        let mut highacc = 0.;
        let mut lowacc = 0.;
        let mut volumeacc = 0.;

        let mut aligned = false;
        let mut i = 0;

        for (&ts, row) in self.v.iter() {
            // align with minute mark ("snap to grid")
            //
            //
            // --|------|------|------|-->
            //   |
            //   ^ discard up to this point
            //
            //
            if align && !aligned {
                let snap_point = (ts / (self.scale as Time * 60)) * (self.scale as Time * 60);
                if ts == snap_point {
                    aligned = true;
                    i = 0;
                } else {
                    continue;
                }
            }
            // new candle, reset using first candle
            if i % new_scale as usize == 0 {
                startacc = ts;
                openacc = row.open;
                highacc = row.high;
                lowacc = row.low;
                volumeacc = row.volume;
                i += 1;
                continue;
            }

            // accumulate new high, low and volume
            highacc = highacc.max(row.high);
            lowacc = lowacc.min(row.low);
            volumeacc += row.volume;

            // if it's the last minute, insert
            if (i % (new_scale as usize)) == ((new_scale as usize) - 1) {
                let candle = Candle {
                    start: startacc,
                    end: ts,
                    open: openacc,
                    high: highacc,
                    low: lowacc,
                    close: row.close,
                    volume: volumeacc,
                };

                res.insert(startacc, candle);
            }
            i += 1;
        }

        // sanity check!
        assert_eq!(res.len(), self.v.len() / (new_scale as usize));
        debug_assert!(self._test_epochs_must_be_sequential());

        Some(TimeBars {
            v: res,
            scale: new_scale,
        })
    }
}

/// Check a list of sequence
///
/// Returns maximum continuous sequence
///
/// example: [60, 120, 180] -> [(60, 180)]
///
/// The algorithm does the following:
///     1. a "rolling" conversion:
///         epochs to natural numbers
///         [60, 120, 180] -> [1, 2, 3]
///     2. then minus the index
///         [1, 2 - 1, 3 - 2] -> [1, 1, 1]
///     3. group and count the size of each group
///         [1, 1, 1, 3, 3, 3, 3] -> [3, 4]
///     4. convert back to begin and end epochs
///         [3] => [(60, 180)]
///
/// :param lst: list of epochs
/// :return: list of tuples of shape (start, end)
fn ranges(lst: &Vec<Time>) -> Vec<(Time, Time)> {
    let mut pos = Vec::new();

    for (i, j) in lst.iter().enumerate() {
        pos.push(j / 60 - i as Time);
    }

    let mut ret = Vec::new();
    let mut t = 0;

    // [1, 1, 1, 2, 2] -> [3, 2]
    let n_groups = {
        if pos.len() == 0 {
            vec![]
        } else {
            let mut n_groups = vec![];
            let mut prev = pos[0];
            let mut count = 0;
            for &num in pos.iter() {
                if num != prev {
                    n_groups.push(count);
                    count = 0;
                } else {
                    count += 1;
                }
                prev = num;
            }
            if n_groups.len() == 0 {
                n_groups.push(count);
            } else {
                n_groups.push(count + 1);
            }
            n_groups
        }
    };

    for &l in n_groups.iter() {
        let el = lst.get(t).unwrap();
        t += l;
        ret.push((el.clone(), el + 60 * (l - 1) as Time));
    }

    ret
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candle_to_csv() {
        let inp = Candle {
            start: 0,
            end: 0,
            open: 0.,
            close: 0.,
            high: 0.,
            low: 0.,
            volume: 0.,
        };
        let target = "0,0,0,0,0,0,0";
        assert_eq!(inp.to_csv(), target);
    }

    #[test]
    fn test_candle_snap_to_grid() {
        let mut v = IndexMap::<Time, Candle>::new();
        for i in 30..121 {
            let j = 60 * i;
            v.insert(
                j,
                Candle {
                    start: j,
                    end: j + 60,
                    open: 0.,
                    close: 1.,
                    high: 2.,
                    low: 0.,
                    volume: 1.,
                },
            );
        }

        let candles = TimeBars::new(v, 1);
        let mut tree = IndexMap::new();
        tree.insert(
            1800,
            Candle {
                start: 1800,
                end: 5340,
                open: 0.,
                high: 2.,
                low: 0.,
                close: 1.,
                volume: 60.,
            },
        );

        assert_eq!(
            TimeBars { v: tree, scale: 60 },
            candles.rebin(true, 60).unwrap()
        );
    }

    // #[test]
    // fn assert_same_data() {
    //     static FNAME : &str = "../../test/test-data/bt_btcnav.dtf";
    //     let ups = &super::super::decode(FNAME)[1..100000];

    //     // test two ways
    //     let first = TimeBars::from_updates(false, &ups);
    //     let second = TimeBars::from_updates(true, &ups);

    //     info!("{}", *second.v.iter().next_back().unwrap().0);

    //     for (&ts, row) in first.v.iter() {
    //         if second.v.contains_key(&ts) {
    //             let other = second.v.get(&ts).unwrap();
    //             assert_eq!(row, other);
    //         }
    //     }
    // }

    // #[test]
    // fn assert_two_ways_produce_the_same_continuation_candles() {
    //     static FNAME : &str = "../../test/test-data/bt_btcnav.dtf";
    //     let ups = &super::super::decode(FNAME)[1..100000];

    //     // test two ways
    //     let mut first = TimeBars::from_updates(false, &ups);
    //     first.insert_continuation_candles();

    //     let second = TimeBars::from_updates(true, &ups);
    //     assert_eq!(first, second);
    // }

    #[test]
    fn test_create_new_candles() {
        assert_eq!(
            TimeBars::new(IndexMap::new(), 1),
            TimeBars {
                v: IndexMap::new(),
                scale: 1,
            }
        );
    }

    #[test]
    fn test_fix_missing_candles() {
        let mut v = IndexMap::new();
        for i in 30..121 {
            if i >= 50 && i <= 60 {
                continue;
            }
            let j = 60 * i;

            v.insert(
                j,
                Candle {
                    start: j,
                    end: j + 60,
                    open: 0.,
                    close: 1.,
                    high: 2.,
                    low: 0.,
                    volume: 1.,
                },
            );
        }
        let mut candles = TimeBars::new(v, 1);

        assert_eq!(
            vec![
                3000,
                3060,
                3120,
                3180,
                3240,
                3300,
                3360,
                3420,
                3480,
                3540,
                3600,
            ],
            candles.missing_epochs()
        );
        assert_eq!(vec![(3000, 3600)], candles.missing_ranges());
        candles.insert_continuation_candles();
        assert_eq!(Vec::<Time>::new(), candles.missing_epochs());
        assert_eq!(Vec::<(Time, Time)>::new(), candles.missing_ranges());
    }


    #[test]
    fn test_ranges() {
        let v: Vec<Time> = vec![60, 120, 180, 600, 660, 720];
        let result = ranges(&v);
        let shouldbe: Vec<(Time, Time)> = vec![(60, 180), (600, 720)];
        assert_eq!(shouldbe, result);

        let v: Vec<Time> = vec![0, 60, 120, 180, 240, 600, 660, 720];
        let result = ranges(&v);
        let shouldbe: Vec<(Time, Time)> = vec![(0, 240), (600, 720)];
        assert_eq!(shouldbe, result);
    }

    #[test]
    fn test_must_be_sequential() {
        let mut candles = IndexMap::new();
        for i in 1..10 {
            let j = i * 60;
            candles.insert(
                j,
                Candle {
                    start: j,
                    end: j + 60,
                    open: 0.,
                    close: 0.,
                    high: 0.,
                    low: 0.,
                    volume: 0.,
                },
            );
        }

        let c = TimeBars {
            v: candles.clone(),
            scale: 1,
        };
        assert!(c._test_epochs_must_be_sequential());

        candles.insert(
            10000,
            Candle {
                start: 10000,
                end: 18000,
                open: 0.,
                close: 0.,
                high: 0.,
                low: 0.,
                volume: 0.,
            },
        );
        let g = TimeBars {
            v: candles,
            scale: 1,
        };
        assert!(!g._test_epochs_must_be_sequential());
    }

    #[test]
    fn test_rebin() {
        let mut candles = IndexMap::new();
        let to_scale: usize = 5;
        let upto: usize = 5;
        for i in 1..(upto + 1) {
            let j = i as Time * 60;
            candles.insert(
                j,
                Candle {
                    start: j,
                    end: j + 60,
                    open: 0.,
                    close: 0.,
                    high: 0.,
                    low: 0.,
                    volume: 0.,
                },
            );
        }

        let c = TimeBars {
            v: candles.clone(),
            scale: 1,
        };
        println!("{:?}", c);
        let rebinned = c.rebin(false, to_scale as u16).unwrap();
        println!("{:?}", rebinned);
        assert_eq!(rebinned.scale, to_scale as u16);
        assert_eq!(rebinned.v.len(), upto / to_scale);
    }

    #[test]
    fn should_have_right_attr() {
        use super::super::*;

        let mut candles = IndexMap::new();
        let to_scale: usize = 5;
        let upto: usize = 5;
        for i in 1..(upto + 1) {
            let j = i as Time * 60;
            candles.insert(
                j,
                Candle {
                    start: j,
                    end: j+60,
                    open: 100. * i as Price,
                    close: 100. * i as Price,
                    high: i as Price,
                    low: i as Price,
                    volume: i as Price,
                },
            );
        }

        let c = TimeBars {
            v: candles.clone(),
            scale: 1,
        };
        println!("{:?}", c);
        let rebinned = c.rebin(false, to_scale as u16).unwrap();
        println!("{:?}", rebinned);
        assert_eq!(rebinned.scale, to_scale as u16);
        assert_eq!(rebinned.v.len(), upto / to_scale);


        let mut i = 1;
        for bin in rebinned.v.values() {
            println!("{:?}", bin);
            assert_eq!(bin.high, (i * to_scale) as Price);
            assert_eq!(bin.open, (100 * (i - 1) * to_scale + 100) as Price);
            assert_eq!(bin.close, 100. * (i * to_scale) as Price);
            assert_eq!(
                bin.volume,
                (1 + (i - 1) * to_scale..(i * to_scale + 1)).fold(0, |a, b| a + b) as Price
            );
            i += 1;
        }
    }

}

// ────────────────────────────────────────────────────────────────────────────────