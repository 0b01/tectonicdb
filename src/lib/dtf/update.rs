use std::cmp::Ordering;
use byteorder::{BigEndian, WriteBytesExt};

/// convertion methods for slice of `Update`s
pub trait UpdateVecConvert {
    /// convert into json
    fn as_json(&self) -> String;
    /// convert into csv
    fn as_csv(&self) -> String;
}

impl UpdateVecConvert for [Update] {
    fn as_json(&self) -> String {
        update_vec_to_json(self)
    }
    fn as_csv(&self) -> String {
        update_vec_to_csv(&self)
    }
}

impl UpdateVecConvert for Vec<Update> {
    fn as_json(&self) -> String {
        update_vec_to_json(self)
    }
    fn as_csv(&self) -> String {
        update_vec_to_csv(&self)
    }
}


fn update_vec_to_csv(vecs: &[Update]) -> String {
    let objects: Vec<String> = vecs.into_iter().map(|up| up.as_csv()).collect();
    objects.join("\n")
}

fn update_vec_to_json(vecs: &[Update]) -> String {
    let objects: Vec<String> = vecs.into_iter().map(|up| up.as_json()).collect();
    objects.join(", ")
}


/// Represents an L2 orderbook update
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Copy, Serialize, Deserialize)]
pub struct Update {
    /// time stamp
    pub ts: u64,
    /// sequence number
    pub seq: u32,
    /// is the update for a trade
    pub is_trade: bool,
    /// is the update on the bid or ask side
    pub is_bid: bool,
    /// price of the order
    pub price: f32,
    /// size of the order
    pub size: f32,
}

impl Update {
    /// Serialize to bytearray
    pub fn serialize(&self, ref_ts: u64, ref_seq: u32) -> Vec<u8> {
        if self.seq < ref_seq {
            println!("{:?}", ref_seq);
            println!("{:?}", self);
            panic!("TODO: ???");
            /* TODO */
        }
        let mut buf: Vec<u8> = Vec::new();
        let _ = buf.write_u16::<BigEndian>((self.ts - ref_ts) as u16);
        let _ = buf.write_u8((self.seq - ref_seq) as u8);

        let mut flags = Flags::FLAG_EMPTY;
        if self.is_bid {
            flags |= Flags::FLAG_IS_BID;
        }
        if self.is_trade {
            flags |= Flags::FLAG_IS_TRADE;
        }
        let _ = buf.write_u8(flags.bits());

        let _ = buf.write_f32::<BigEndian>(self.price);
        let _ = buf.write_f32::<BigEndian>(self.size);
        buf
    }

    /// Convert to json string
    pub fn as_json(&self) -> String {
        format!(
            r#"{{"ts":{},"seq":{},"is_trade":{},"is_bid":{},"price":{},"size":{}}}"#,
            (self.ts as f64) / 1000_f64,
            self.seq,
            self.is_trade,
            self.is_bid,
            self.price,
            self.size
        )
    }

    /// Convert to csv string
    pub fn as_csv(&self) -> String {
        format!(
            r#"{},{},{},{},{},{}"#,
            (self.ts as f64) / 1000_f64,
            self.seq,
            self.is_trade,
            self.is_bid,
            self.price,
            self.size
        )
    }
}

impl PartialOrd for Update {
    fn partial_cmp(&self, other: &Update) -> Option<Ordering> {
        let selfts = self.ts;
        let otherts = other.ts;
        if selfts > otherts {
            Some(Ordering::Greater)
        } else if selfts == otherts {
            Some(self.seq.cmp(&other.seq))
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
    /// tightly packed bitflag representation of boolean values in the update struct
    pub struct Flags: u8 {
        /// empty
        const FLAG_EMPTY   = 0b0000_0000;
        /// update.is_bid
        const FLAG_IS_BID   = 0b0000_0001;
        /// update.is_trade
        const FLAG_IS_TRADE = 0b0000_0010;
    }
}

impl Flags {
    /// convert to bool
    pub fn to_bool(&self) -> bool {
        (self.bits == 0b0000_0001) || (self.bits == 0b0000_0010)
    }
}
