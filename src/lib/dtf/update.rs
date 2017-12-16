use std::cmp::Ordering;
use byteorder::{BigEndian, WriteBytesExt};

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub ts: u64,
    pub seq: u32,
    pub is_trade: bool,
    pub is_bid: bool,
    pub price: f32,
    pub size: f32,
}


impl Update {

    pub fn serialize(&self, ref_ts : u64, ref_seq : u32) -> Vec<u8> {
        if self.seq < ref_seq {
            println!("{:?}", ref_seq);
            println!("{:?}", self);
            panic!("TODO: ???");
            /* TODO */
        }
        let mut buf : Vec<u8> = Vec::new();
        let _ = buf.write_u16::<BigEndian>((self.ts- ref_ts) as u16);
        let _ = buf.write_u8((self.seq - ref_seq) as u8);

        let mut flags = Flags::FLAG_EMPTY;
        if self.is_bid { flags |= Flags::FLAG_IS_BID; }
        if self.is_trade { flags |= Flags::FLAG_IS_TRADE; }
        let _ = buf.write_u8(flags.bits());

        let _ = buf.write_f32::<BigEndian>(self.price);
        let _ = buf.write_f32::<BigEndian>(self.size);
        buf
    }

    pub fn to_json(&self) -> String {
        format!(r#"{{"ts":{},"seq":{},"is_trade":{},"is_bid":{},"price":{},"size":{}}}"#,
                  (self.ts as f64) / 1000_f64, self.seq, self.is_trade, self.is_bid, self.price, self.size)
    }

    pub fn to_csv(&self) -> String {
        format!(r#"{},{},{},{},{},{}"#,
                  (self.ts as f64) / 1000_f64, self.seq, self.is_trade, self.is_bid, self.price, self.size)
    }
}

impl PartialOrd for Update {
    fn partial_cmp(&self, other : &Update) -> Option<Ordering> {
        let selfts = self.ts;
        let otherts = other.ts;
        if selfts > otherts {
            Some(Ordering::Greater)
        } else if selfts == otherts {
            Some(Ordering::Equal)
        } else {
            Some(Ordering::Less)
        }
    }
}

impl Ord for Update {
    fn cmp(&self, other: &Update) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Eq for Update {}


bitflags! {
    pub struct Flags: u8 {
        const FLAG_EMPTY   = 0b0000_0000;
        const FLAG_IS_BID   = 0b0000_0001;
        const FLAG_IS_TRADE = 0b0000_0010;
    }
}

impl Flags {
    pub fn to_bool(&self) -> bool {
        (self.bits == 0b0000_0001) || (self.bits == 0b0000_0010)
    }
}
