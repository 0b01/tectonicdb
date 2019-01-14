/// this module handles orderbook operations

use indexmap::IndexMap;
use crate::postprocessing::histogram::{Histogram, BinCount};
use crate::dtf::update::Update;
use std::fmt;
use std::f64;

type PriceBits = u64;
type Size = f32;
type Time = u64;
type OrderbookSide = IndexMap<PriceBits, Size>;

/// data structure for orderbook
#[derive(Clone)]
pub struct Orderbook {
    /// bids side
    pub bids: OrderbookSide,
    /// asks side
    pub asks: OrderbookSide,
}

impl Orderbook {
    /// Create empty orderbook
    pub fn new() -> Orderbook {
        Orderbook {
            bids: IndexMap::new(),
            asks: IndexMap::new(),
        }
    }

    /// Remove zero levels from books
    pub fn clean(&mut self) {
        self.bids = self.bids.iter()
                .map(|(&a,&b)| (a,b))
                .filter(|&(_p,s)|s != 0.)
                .collect::<IndexMap<PriceBits, Size>>();
        self.asks = self.asks.iter()
                .map(|(&a,&b)| (a,b))
                .filter(|&(_p,s)|s != 0.)
                .collect::<IndexMap<PriceBits, Size>>();
    }
}

impl fmt::Debug for Orderbook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "bids:\n");
        for (&price, size) in self.bids.iter() {
            let _ = write!(
                f,
                "- price: {} \t - size: {}\n",
                f64::from_bits(price),
                size
            );
        }
        let _ = write!(f, "\n");

        let _ = write!(f, "asks:\n");
        for (&price, size) in self.asks.iter() {
            let _ = write!(
                f,
                "- price: {} \t - size: {}\n",
                f64::from_bits(price),
                size
            );
        }
        write!(f, "\n")
    }
}


/// Data structure for rebinning orderbooks
///
/// If you think of an order as a 2D image, rebinning is lowering the resolution
/// For example, the raw orderbook is 1 nano second apart, you can "zoom out" to 1 second
///
/// Price rebinning is similar.
pub struct RebinnedOrderbook {
    /// a map from time to orderbook
    pub book: IndexMap<u64, Orderbook>,
    /// histogram of price
    pub price_hist: Histogram,
}

impl RebinnedOrderbook {
    /// convert a list of updates to rebinned orderbook with fixed number of time steps bins and ticks bins
    pub fn from(ups: &[Update], step_bins: BinCount, tick_bins: BinCount, m: f64) -> RebinnedOrderbook {

        // build histogram so later can put price and time into bins
        let (price_hist, step_hist) = Histogram::from(&ups, step_bins, tick_bins, m);

        // raw_price -> size
        // using a fine_level to track individual price level instead of a batched one
        let mut fine_level = Orderbook::new();
        // coarse grained books, temp_ob keeps track of current level
        // coarse means rebinned(like snap to grid)
        let mut temp_ob = Orderbook::new();
        // coarse price orderbook across coarse time
        let mut ob_across_time = IndexMap::<Time, Orderbook>::new();

        // iterate over each update
        for up in ups.iter() {
            // ignore trades, since there should be an accompanying level update
            if up.is_trade {
                continue;
            }

            // rebinned ts, price
            let ts = step_hist.to_bin((up.ts / 1000) as f64);
            let price = price_hist.to_bin(up.price as f64);

            // if is an outlier, don't update orderbook
            if ts == None || price == None {
                continue;
            }
            let coarse_time = ts.unwrap().to_bits();
            let coarse_price = price.unwrap().to_bits();

            // get coarse_size and update local book
            let coarse_size = {
                // get fine-grained size
                // if the fine price does not exist in the dict, insert the current size
                // returns a mutable reference
                fine_level.clean();
                let fine_book = if up.is_bid {
                    &mut fine_level.bids
                } else {
                    &mut fine_level.asks
                };
                let fine_size = fine_book.entry((up.price as f64).to_bits()).or_insert(
                    up.size,
                );

                // coarse_size is the size at coarse_price
                let local_side = if up.is_bid {
                    &mut temp_ob.bids
                } else {
                    &mut temp_ob.asks
                };
                let coarse_size = (*local_side).entry(coarse_price).or_insert(up.size);

                if (*fine_size) == up.size {
                    // if level was 0, fine_size == coarse_size == up.size
                    () // do nothing
                } else if (*fine_size) > up.size {
                    // if size shrinks
                    *coarse_size -= (*fine_size) - up.size; // shrink the coarse size
                } else
                /* if (*fine_size) < up.size */
                {
                    // if size grows
                    *coarse_size += up.size - (*fine_size); // grow the coarse size
                }

                *fine_size = up.size;

                // XXX: important
                // there might be orders before the first cancellation
                // we simply ignore those by setting the size to 0
                if *coarse_size < 0. {
                    *coarse_size = 0.;
                }

                *coarse_size
            };

            // if the current coarse_time is not in orderbook
            if !ob_across_time.contains_key(&coarse_time) {
                // insert a copy of current book
                // temp_ob.clean();
                ob_across_time.insert(coarse_time, temp_ob.clone());
            } else {
                // if already in global, modify the orderbook at ts
                let ob_at_time = ob_across_time.get_mut(&coarse_time).unwrap();
                let global_side = if up.is_bid {
                    &mut ob_at_time.bids
                } else {
                    &mut ob_at_time.asks
                };
                (*global_side).insert(coarse_price, coarse_size);
            }
        }

        for v in ob_across_time.values_mut() {
            v.clean();
        }

        RebinnedOrderbook {
            book: ob_across_time,
            price_hist: price_hist,
        }
    }
}

impl fmt::Debug for RebinnedOrderbook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (&ts, ob) in self.book.iter() {
            let _ = write!(f, "ts: {}\n", f64::from_bits(ts));
            let _ = write!(f, "{:?}\n", ob);
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dtf;
    static FNAME: &str = "test/test-data/bt_btcnav.dtf";

    #[test]
    fn test_level_orderbook() {
        let step_bins = 100;
        let tick_bins = 100;

        let ups = dtf::file_format::decode(FNAME, Some(1000)).unwrap();
        let ob = RebinnedOrderbook::from(ups.as_slice(), step_bins, tick_bins, 2.);

        assert_eq!(ob.book.len(), step_bins - 1);
        for v in ob.book.values() {
            assert!(v.bids.values().len() < tick_bins);
            assert!(v.asks.values().len() < tick_bins);
        }

        assert_eq!(
            format!("{:?}", ob.book.values().next_back().unwrap()),
            "".to_owned()+
"bids:
- price: 0.00010943656738475906 	 - size: 266.0109
- price: 0.00011011131470576117 	 - size: 600
- price: 0.00010923414318845842 	 - size: 157.5617
- price: 0.00010936909265265885 	 - size: 269.10645
- price: 0.00010754727488595314 	 - size: 587.0855
- price: 0.00010997636524156074 	 - size: 69.96973
- price: 0.00011132585988356497 	 - size: 247.91327
- price: 0.00011004383997366096 	 - size: 149.29991

asks:
- price: 0.00011173070827616624 	 - size: 1768.4296
- price: 0.00011098848622306392 	 - size: 0.00012207031

"
,
        );
    }
}
