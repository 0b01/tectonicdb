// this module handles orderbook ops on Updates
use std::collections::BTreeMap;
use super::utils::price_histogram::{Histogram, Count};

// type Price = f32;
type PriceBits = u32;
type Size = f32;
type Time = u32;
type OrderbookSide = BTreeMap<PriceBits, Size>;

#[derive(Clone, Debug)]
struct Orderbook {
    bids: OrderbookSide,
    asks: OrderbookSide
}

impl Orderbook {
    fn new() -> Orderbook {
        Orderbook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new()
        }
    }

    fn clean(&mut self) {
        self.bids = self.bids.iter()
                .map(|(&a,&b)| (a,b))
                .filter(|&(_p,s)|s != 0.)
                .collect::<BTreeMap<PriceBits, Size>>();
        self.asks = self.asks.iter()
                .map(|(&a,&b)| (a,b))
                .filter(|&(_p,s)|s != 0.)
                .collect::<BTreeMap<PriceBits, Size>>();
    }
}

#[derive(Debug)]
struct RebinnedOrderbook(BTreeMap<Time, Orderbook>);

impl RebinnedOrderbook {
    fn from(ups: &[super::Update], step_bins: Count, tick_bins: Count) -> RebinnedOrderbook {

        let (price_hist, step_hist) = Histogram::from(&ups, step_bins, tick_bins);

        let mut temp_ob = Orderbook::new();
        let mut ob_across_time = BTreeMap::<Time, Orderbook>::new();
        for up in ups.iter() {
            if up.is_trade { continue; }
            let ts = (up.ts / 1000) as u32;
            if !ob_across_time.contains_key(&ts) {
                temp_ob.clean();
                {
                    // update local orderbook
                    let local_side = if up.is_bid {&mut temp_ob.bids} else {&mut temp_ob.asks};
                    (*local_side).insert(up.price.to_bits(), up.size);
                }
                // copy local orderbook to global
                ob_across_time.insert(ts, temp_ob.clone());
            }
        }

        RebinnedOrderbook(ob_across_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::*;
    static FNAME: &str = "test-data/bt_btcnav.dtf";

    // #[test]
    // fn test_level_orderbook() {
    //     let ups = dtf::decode(FNAME, Some(100));
    //     let ob = Orderbooks::from(ups.as_slice());
    //     println!("{:?}", ob);
    // }
}