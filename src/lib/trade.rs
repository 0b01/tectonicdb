use std::collections::BTreeMap;

type Trade = super::Update;
type Time = u32;

struct Trades {
    trades: BTreeMap<Time, Trade>
}

impl<'a> From<&'a [super::Update]> for Trades {
    fn from(ups: &[super::Update]) -> Trades {
        let mut trades = BTreeMap::new();
        for up in ups.iter() {
            if up.is_trade {
                trades.insert((up.ts / 1000) as u32, up.clone());
            }
        }

        Trades {
            trades
        }
    }
}

