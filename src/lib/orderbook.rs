// this module handles orderbook ops on Updates
use std::collections::BTreeMap;

type Price = f32;
type Size = f32;
type Scale = u16;
type Time = u32;
type OrderbookSide = BTreeMap<Price, Size>;

struct Orderbook {
    bids: OrderbookSide,
    asks: OrderbookSide
}

impl Orderbook {
    fn new() -> Orderbook {
        unimplemented!()
    }
}

struct Orderbooks {
    books: BTreeMap<Time, Orderbook>,
    scale: Scale,
}

impl<'a> From<&'a [super::Update]> for Orderbooks {
    fn from(ups: &[super::Update]) -> Orderbooks {
        unimplemented!()
    }
}

