// this module handles orderbook ops on Updates
use std::collections::BTreeMap;

type Price = f32;
type Size = f32;
type Scale = u16;
type Time = u32;
type OrderbookSide = BTreeMap<Price, Size>;

struct Orderbook {
    bids: OrderbookSide,
    asks: OrderbookSide,
    time: Time,
    scale: Scale
}

impl Orderbook {
    fn new() -> Orderbook {
        unimplemented!();
    }
}