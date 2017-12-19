use std::collections::{BTreeMap};
use dtf::Update;

type Time = u64;

enum EventType {
    CancelEvent, TradeEvent, CreateEvent
}

#[derive(Debug)]
struct Events {
    cancelled: BTreeMap<Time, Vec<Update>>,
    trades: BTreeMap<Time, Vec<Update>>,
    created: BTreeMap<Time, Vec<Update>>
}

impl<'a> From<&'a[Update]> for Events {
    fn from(ups: &[Update]) -> Events {

        let mut cancelled = BTreeMap::new();
        let mut trades = BTreeMap::new();
        let mut created = BTreeMap::new();

        let mut current_level = BTreeMap::new();

        for row in ups {

            let ts = row.ts;
            let price = row.price.to_bits();

            if row.is_trade {
                let v = trades.entry(ts).or_insert(Vec::new());
                (*v).push(row.clone());
            } else {
                let prev = if current_level.contains_key(&price) 
                            { *current_level.get(&price).unwrap() }
                           else
                            { 0. };
                if row.size == 0. || row.size <= prev {
                    let v = cancelled.entry(ts).or_insert(Vec::new());
                    (*v).push(row.clone());
                } else if row.size > prev {
                    let v = created.entry(ts).or_insert(Vec::new());
                    (*v).push(row.clone());
                } else { // size == prev
                    unreachable!();
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

impl Events {
    pub fn filter_volume(&self, event_type : EventType, from_vol: f32, to_vol: f32)
            -> Vec<Update> {
        let obj = match event_type {
            EventType::CancelEvent => &self.cancelled,
            EventType::CreateEvent => &self.created,
            EventType::TradeEvent => &self.trades
        };

        let mut ret = Vec::new();
        for v in obj.values() {
            for up in v.iter() {
                if up.size >= from_vol
                && up.size <= to_vol {
                    ret.push(up.clone());
                }
            }
        }
        ret
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use dtf;
    static FNAME : &str = "test/test-data/bt_btcnav.dtf";
    static POLO : &str = "test/test-data/pl_btc_nav.dtf";

    #[test]
    fn test_into_events (){
        // let records = dtf::decode(FNAME, Some(10000));
        // let ups = records.as_slice();
        // TODO: Finish this test...
    }

    #[test]
    fn test_volume_filter() {
        let records = dtf::decode(FNAME, Some(10000));
        let ups = records.as_slice();

        let evts = Events::from(ups);

        let cancels = evts.filter_volume(EventType::CancelEvent, 100., 200.);
        assert!(cancels.len() > 0);
        for up in cancels.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }

        let creates = evts.filter_volume(EventType::CreateEvent, 100., 200.);
        assert!(creates.len() > 0);
        for up in creates.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }

        let trades = evts.filter_volume(EventType::TradeEvent, 100., 200.);
        assert!(trades.len() > 0);
        for up in trades.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }
    }

    #[test]
    fn should_work_with_poloniex_too() {
        // TODO: more poloniex tests
        // The raw data doesn't look correct.
        let records = dtf::decode(POLO, Some(10000));

        let ups = records.as_slice();
        let evts = Events::from(ups);

        let trades = evts.filter_volume(EventType::TradeEvent, 100., 200.);
        assert!(trades.len() > 0);
        for up in trades.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }
    }
}
