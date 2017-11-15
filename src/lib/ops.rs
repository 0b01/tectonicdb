// this module handles orderbook ops on Updates
use std::collections::BTreeMap;

type Price = f32;
type Size = f32;

type Time = u32;

struct Orderbook {
    bids: BTreeMap<Price, Size>,
    asks: BTreeMap<Price, Size>,
    time: Time
}

impl Orderbook {
    fn new() -> Orderbook {
        unimplemented!();
    }
}