use std::collections::{BTreeMap};

type Time = u64;

#[derive(Debug)]
struct Events {
    cancelled: BTreeMap<Time, super::Update>,
    trades: BTreeMap<Time, super::Update>,
    created: BTreeMap<Time, super::Update>
}

impl<'a> From<&'a[super::Update]> for Events {
    fn from(ups: &[super::Update]) -> Events {

        let mut cancelled = BTreeMap::new();
        let mut trades = BTreeMap::new();
        let mut created = BTreeMap::new();

        let mut current_level = BTreeMap::new();

        for row in ups.iter() {

            let ts = row.ts;
            let price = row.price.to_bits();

            if row.is_trade {
                trades.insert(ts, row.clone());
            } else {
                let prev = if current_level.contains_key(&price) 
                            { *current_level.get(&price).unwrap() }
                           else
                            { 0. };
                if row.size == 0. || row.size <= prev {
                    cancelled.insert(ts, row.clone());
                } else if row.size > prev {
                    created.insert(ts, row.clone());
                } else { // size == prev
                    panic!("IMPOSSIBLE");
                }
            }

            current_level.insert(price, row.size);
        }

        Events {
            cancelled,
            trades,
            created
        }
    }
}

#[cfg(test)]
mod test {

    use super::Events;

    #[test]
    fn test_into_events (){
        static FNAME : &str = "test-data/bt_btcnav.dtf";
        let ups = &super::super::decode(FNAME)[1..100];

        let events = Events::from(ups);
        // println!("{:?}", events);

        // TODO: Finish this test...
    }
}