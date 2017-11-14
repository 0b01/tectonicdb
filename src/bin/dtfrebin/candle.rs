use std::collections::{BTreeMap, HashSet};
use itertools::Itertools;
use dtf;

#[derive(Clone, Debug, PartialEq)]
pub struct Candles {
    v: BTreeMap<u32, Candle>,
    scale: u16
}

impl Candles {
    pub fn to_csv(self) -> String {
        let csvs : Vec<String> = self.v.values().into_iter()
                .map(|candle| candle.to_csv())
                .collect();

        csvs.join("\n")
    }

    /// find missing epochs
    fn missing_epochs(&self) -> Vec<u32> {

        let mut set = HashSet::<u32>::new();
        let mut missing = Vec::<u32>::new();
        
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
            it += (self.scale as u32) * 60;
        }

        missing
    }

    /// return ranges of missing epochs
    pub fn missing_ranges(&self) -> Vec<(u32, u32)> {
        ranges(&self.missing_epochs())
    }

    /// insert continuation candles and fix missing
    pub fn insert_continuation_candles(&mut self) {
        let (mut last_ts, last_close) = {
            let (&last_ts, first) = self.v.iter().next().unwrap();
            let last_close = first.close;
            (last_ts, last_close)
        };

        let mut temp = BTreeMap::<u32,Candle>::new();

        for &ts in self.v.keys() {
            if (ts != last_ts + 60) && (last_ts != 0) && (last_ts != ts) {
                //insert continuation candle(s)
                let mut cur = last_ts;
                while cur < ts {
                    temp.insert(cur, Candle {
                        volume: 0.,
                        high: last_close,
                        low: last_close,
                        open: last_close,
                        close: last_close
                    });
                    cur += 60;
                }
            }
            last_ts = ts;
        }

        self.v.extend(temp);
    }


    /// create new Candles object
    pub fn new(v: BTreeMap<u32, Candle>, scale: u16) -> Candles {
        let ret = Candles {
            v,
            scale
        };

        // assert!(self._test_epochs_must_be_sequential());
        ret
    }

    /// epochs must be exactly incrementing by n * 60
    fn _test_epochs_must_be_sequential(&self) -> bool {
        // all([a[0] + i * 60 * minutes == x for i, x in enumerate(a)])
        let mut i : u32 = 0;
        let &first = self.v.keys().next().unwrap();
        for &row in self.v.keys() {
            if first + i * 60 * (self.scale as u32) != row {
                return false;
            }
            i += 1;
        }
        true
    }


    pub fn from_updates(fix_missing: bool, ups: &[dtf::Update]) -> Candles {
        let mut last_ts = 0;        // store the last timestep to test continuity
        let mut last_close = 0.;    // 

        let mut candles : BTreeMap<u32, Candle> = BTreeMap::new();

        for trade in ups.iter() {
            if !trade.is_trade { continue; }
            // floor(ts)
            let ts = (dtf::fill_digits(trade.ts) / 1000 / 60 * 60) as u32; 
            
            if fix_missing && (ts != last_ts + 60) && (last_ts != 0) && (last_ts != ts) {
                //insert continuation candle(s)
                let mut cur = last_ts;
                while cur < ts {
                    candles.insert(cur, Candle {
                        volume: 0.,
                        high: last_close,
                        low: last_close,
                        open: last_close,
                        close: last_close
                    });
                    cur += 60;
                }
            }

            let new_candle = if candles.contains_key(&ts) {
                let c = candles.get(&ts).unwrap();

                Candle {
                    volume: c.volume + trade.size,
                    high: if trade.price > c.high { trade.price } else { c.high },
                    low: if trade.price < c.low  { trade.price } else { c.low },
                    close: trade.price,
                    open: c.open
                }
            } else {
                Candle {
                    volume: trade.size,
                    high: trade.price,
                    low: trade.price,
                    close: trade.price,
                    open: trade.price
                }
            };

            candles.insert(ts, new_candle);
            last_ts = ts;
            last_close = trade.price;
        }

        return Candles::new(
            candles,
            1
        );
    }

    /// rebin 1 minute candles to x-minute candles
    pub fn rebin(self, align: bool, new_scale : u16) -> Option<Candles> {
        if new_scale < self.scale { return None }
        else if new_scale == self.scale { return Some(self) }

        let mut res = BTreeMap::<u32,Candle>::new();

        let mut startacc = 0;
        let mut openacc = 0.;
        let mut highacc = 0.;
        let mut lowacc = 0.;
        let mut volumeacc = 0.; 

        let mut aligned = false;
        let mut i = 0;

        for (&ts, row) in self.v.iter() {
            // align with minute mark ("snap" to grid)
            //
            //
            // --|------|------|------|-->
            //   |
            //   ^ discard up to this point         
            //
            //
            if align && !aligned {
                let snap_point = (ts / (self.scale as u32 * 60)) * (self.scale as u32 * 60);
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
            highacc =  if row.high > highacc {row.high} else {highacc};
            lowacc = if row.low < lowacc {row.low} else {lowacc};
            volumeacc += row.volume;

            // if it's the last minute, insert
            if (i % (new_scale as usize)) == ((new_scale as usize) - 1 ){
                let candle = Candle {
                    open: openacc,
                    high: highacc,
                    low: lowacc,
                    close: row.close,
                    volume: volumeacc
                };
                
                res.insert(startacc, candle);
            }
            i += 1;
        }

        assert_eq!(res.len(), self.v.len() / (new_scale as usize));
        assert!(self._test_epochs_must_be_sequential());

        Some(Candles {
            v: res,
            scale: new_scale
        })
    }
}

#[derive(PartialOrd, PartialEq, Clone, Debug)]
pub struct Candle {
    open:   f32,
    high:   f32,
    low:    f32,
    close:  f32,
    volume: f32
}

impl Eq for Candle {}

impl Candle {
    fn to_csv(&self) -> String{
        format!("{},{},{},{},{}",
                self.open, self.high, self.low, self.close, self.volume)
    }
}

/// Check a list of sequence
/// Returns maximum continuous sequence
/// [1,2,3,5,6,7] -> [(1,3), (5,7)]
/// :param lst: list of epochs
/// :return: list of tuples of shape (start, end)
fn ranges(lst: &Vec<u32>) -> Vec<(u32, u32)>{
    let mut pos = Vec::new();

    for (i, j) in lst.iter().enumerate() {
        pos.push(j/ 60 - i as u32);
    }

    let mut ret = Vec::new();
    let mut t = 0;
    for (_i, els) in &pos.into_iter().group_by(|e| *e) {
        let l = els.count();
        let el = lst.get(t).unwrap();
        t += l;
        ret.push((el.clone(), el + 60 * (l-1) as u32));
    }

    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candle_to_csv() {
        let inp = Candle {
            open: 0.,
            close: 0.,
            high: 0.,
            low: 0.,
            volume: 0.,
        };
        let target = "0,0,0,0,0";
        assert_eq!(inp.to_csv(), target);
    }

    #[test]
    fn test_candle_snap_to_grid() {
        let mut v = BTreeMap::<u32,Candle>::new();
        for i in 30..121 {
            let j = 60 * i;
            v.insert(j, Candle {
                open: 0.,
                close: 1.,
                high: 2.,
                low: 0.,
                volume: 1.
            });
        }

        let candles = Candles::new(v, 1);
        let mut tree = BTreeMap::new();
        tree.insert(1800, Candle {
                open: 0.,
                high: 2.,
                low: 0.,
                close: 1.,
                volume: 60.
            });

        assert_eq!(Candles {
            v: tree,
            scale: 60
        }, candles.rebin(true, 60).unwrap());
    }

    #[test]
    fn test_create_new_candles() {
        assert_eq!(Candles::new(BTreeMap::new(), 1), Candles {v: BTreeMap::new(), scale:1});
    }

    #[test]
    fn test_fix_missing_candles() {
        let mut v = BTreeMap::new();
        for i in 30..121 {
            if i >= 50 && i <= 60 {
                continue;
            }
            let j = 60 * i;

            v.insert(j, Candle {
                open: 0.,
                close: 1.,
                high: 2.,
                low: 0.,
                volume: 1.
            });
        }
        let mut candles = Candles::new(v, 1);

        assert_eq!(vec![3000, 3060, 3120, 3180, 3240, 3300, 3360, 3420, 3480, 3540, 3600], candles.missing_epochs());
        assert_eq!(vec![(3000, 3600)], candles.missing_ranges());
        candles.insert_continuation_candles();
        assert_eq!(Vec::<u32>::new(), candles.missing_epochs());
        assert_eq!(Vec::<(u32,u32)>::new(), candles.missing_ranges());
    }


    #[test]
    fn test_ranges() {
        let v : Vec<u32> = vec![60,120,180,600,660,720];
        let result = ranges(&v);
        let shouldbe : Vec<(u32,u32)> = vec![(60,180), (600, 720)];
        assert_eq!(shouldbe, result);

        let v : Vec<u32> = vec![0,60,120,180,240,600,660,720];
        let result = ranges(&v);
        let shouldbe : Vec<(u32,u32)> = vec![(0,240), (600, 720)];
        assert_eq!(shouldbe, result);
    }

    #[test]
    fn test_must_be_sequential() {
        let mut candles = BTreeMap::new();
        for i in 1..10 {
            let j = i * 60;
            candles.insert(j, Candle {
                open: 0.,
                close: 0.,
                high: 0.,
                low: 0.,
                volume: 0.
            });
        }

        let c = Candles { v: candles.clone(), scale: 1};
        assert!(c._test_epochs_must_be_sequential());

        candles.insert(10000, Candle {
            open: 0.,
            close: 0.,
            high: 0.,
            low: 0.,
            volume: 0.
        });
        let g = Candles { v: candles, scale: 1};
        assert!(!g._test_epochs_must_be_sequential());
    }

    #[test]
    fn test_rebin() {
        let mut candles = BTreeMap::new();
        let to_scale :usize= 5;
        let upto :usize = 5;
        for i in 1..(upto+1) {
            let j = i as u32 * 60;
            candles.insert(j, Candle {
                open: 0.,
                close: 0.,
                high: 0.,
                low: 0.,
                volume: 0.
            });
        }

        let c = Candles { v: candles.clone(), scale: 1};
        println!("{:?}", c);
        let rebinned = c.rebin(false, to_scale as u16).unwrap();
        println!("{:?}", rebinned);
        assert_eq!(rebinned.scale, to_scale as u16);
        assert_eq!(rebinned.v.len(), upto / to_scale);
    }

    #[test]
    fn should_have_right_attr() {
        let mut candles = BTreeMap::new();
        let to_scale :usize= 5;
        let upto :usize = 5;
        for i in 1..(upto+1) {
            let j =  i as u32 * 60;
            candles.insert(j, Candle {
                open: 100.*i as f32,
                close: 100.*i as f32,
                high: i as f32,
                low: i as f32,
                volume: i as f32
            });
        }

        let c = Candles { v: candles.clone(), scale: 1};
        println!("{:?}", c);
        let rebinned = c.rebin(false, to_scale as u16).unwrap();
        println!("{:?}", rebinned);
        assert_eq!(rebinned.scale, to_scale as u16);
        assert_eq!(rebinned.v.len(), upto / to_scale);


        let mut i = 1;
        for bin in rebinned.v.values() {
            println!("{:?}", bin);
            assert_eq!(bin.high, (i * to_scale) as f32);
            assert_eq!(bin.open, (100 * (i-1) * to_scale + 100) as f32);
            assert_eq!(bin.close, 100. * (i * to_scale) as f32);
            assert_eq!(bin.volume, (1+(i-1)*to_scale..(i*to_scale + 1)).fold(0, |a,b| a+b) as f32);
            i += 1;
        }
    }

}