// File format for Dense Tick Format (DTF)

// File Type:
// Offset 00: magic value 0x4454469001
// Offset 05: Symbol
// Offset 13: how many records
// Offset 21: latest ts //TODO:implement
// Offset 80: columns


extern crate byteorder;

use std::str;
use std::cmp::Ordering;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::fs::File;
use std::io::{
    Write,
    Read,
    Seek,
    BufWriter,
    BufReader,
    SeekFrom
};

static MAGIC_VALUE : &[u8] = &[0x44, 0x54, 0x46, 0x90, 0x01]; // DTF9001
static MAIN_OFFSET : u64 = 80; // main section start at 80
static ITEM_OFFSET : u64 = 22; // each item has 22 bytes

#[derive(Debug)]
pub struct Update {
    ts: u64,
    seq: u32,
    is_trade: bool,
    is_bid: bool,
    price: f32,
    size: f32,
}

impl Ord for Update {
    fn cmp(&self, other: &Update) -> Ordering {
        return self.partial_cmp(&other).unwrap();
    }
}
impl Eq for Update {}

impl PartialOrd for Update {
    fn partial_cmp(&self, other : &Update) -> Option<Ordering> {
        if self.seq > other.seq {
            return Some(Ordering::Greater);
        } else if self.seq == other.seq {
            return Some(Ordering::Equal);
        } else {
            return Some(Ordering::Less);
        }
    }
}

impl PartialEq for Update {
    fn eq(&self, other: &Update) -> bool {
        (other.ts == self.ts)
        && (other.seq == self.seq)
        && (other.is_trade == self.is_trade)
        && (other.is_bid == self.is_bid)
        && (other.price == self.price)
        && (other.size == self.size)
    }
}

pub fn serialize_update(update : &Update) -> Vec<u8> {
    let mut buf : Vec<u8> = Vec::new();
    buf.write_u64::<BigEndian>(update.ts).unwrap();
    buf.write_u32::<BigEndian>(update.seq).unwrap();
    buf.write_u8(update.is_trade as u8).unwrap();
    buf.write_u8(update.is_bid as u8).unwrap();
    buf.write_f32::<BigEndian>(update.price).unwrap();
    buf.write_f32::<BigEndian>(update.size).unwrap();
    buf
}


pub fn encode(fname : &String, symbol : &String, ts : &Vec<Update>) {
    let new_file = File::create(fname).unwrap();
    let mut writer = BufWriter::new(new_file);
    // headers
    writer.write(MAGIC_VALUE).unwrap();

    assert!(symbol.len() <= 8);
    let padded_symbol = format!("{:8}", symbol);
    assert!(padded_symbol.len() == 8);

    // write symbol
    writer.write(padded_symbol.as_bytes()).unwrap();

    // how many records
    writer.write_u64::<BigEndian>(ts.len() as u64).expect("length of records");

    writer.seek(SeekFrom::Start(MAIN_OFFSET)).unwrap();
    for elem in ts.into_iter() {
        let serialized = serialize_update(&elem);
        writer.write(serialized.as_slice()).unwrap();
    }
    writer.flush().expect("FAILURE TO FLUSH");
}

pub fn get_symbol(fname: &String) -> String {
    let file = File::open(fname).expect("OPENING FILE");
    let mut rdr = BufReader::new(file);

    // magic value
    let mut buf = vec![0u8; 5];
    rdr.read_exact(&mut buf).unwrap();
    if buf != MAGIC_VALUE {
        panic!("MAGIC VALUE INCORRECT");
    }

    // read symbol
    let mut buffer = [0; 8];
    rdr.read_exact(&mut buffer).unwrap();
    let symbol = str::from_utf8(&buffer).unwrap().to_owned();

    symbol
}

pub fn read_first(fname: &String) -> Update {
    let file = File::open(fname).expect("OPENING FILE");
    let mut rdr = BufReader::new(file);

    // magic value
    let mut buf = vec![0u8; 5];
    rdr.read_exact(&mut buf).unwrap();
    if buf != MAGIC_VALUE {
        panic!("MAGIC VALUE INCORRECT");
    }

    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

    let current_update = Update {
        ts: rdr.read_u64::<BigEndian>().expect("ts"),
        seq: rdr.read_u32::<BigEndian>().expect("seq"),
        is_trade: rdr.read_u8().expect("is_trade") == 0x00000001,
        is_bid: rdr.read_u8().expect("is_bid") == 0x00000001,
        price: rdr.read_f32::<BigEndian>().expect("price"),
        size: rdr.read_f32::<BigEndian>().expect("size")
    };

    current_update
}

pub fn decode(fname: &String) -> Vec<Update> {
    let mut v : Vec<Update> = Vec::new();
    let file = File::open(fname).expect("OPENING FILE");
    let mut rdr = BufReader::new(file);

    // magic value
    let mut buf = vec![0u8; 5];
    rdr.read_exact(&mut buf).unwrap();
    if buf != MAGIC_VALUE {
        panic!("MAGIC VALUE INCORRECT");
    }

    // read symbol
    let mut buffer = [0; 8];
    rdr.read_exact(&mut buffer).unwrap();
    let _ = str::from_utf8(&buffer).unwrap();
    // println!("{}", symbol);

    // read length

    let nums = rdr.read_u64::<BigEndian>().expect("length of records");

    for n in 0..nums {
        rdr.seek(SeekFrom::Start(MAIN_OFFSET + n * ITEM_OFFSET)).expect("SEEKING");
        let current_update = Update {
            ts: rdr.read_u64::<BigEndian>().expect("ts"),
            seq: rdr.read_u32::<BigEndian>().expect("seq"),
            is_trade: rdr.read_u8().expect("is_trade") == 0x00000001,
            is_bid: rdr.read_u8().expect("is_bid") == 0x00000001,
            price: rdr.read_f32::<BigEndian>().expect("price"),
            size: rdr.read_f32::<BigEndian>().expect("size")
        };
        v.push(current_update);
    }
    v
}

fn main() {
    let fname = "test.bin".to_owned();
    decode(&fname);
}

#[cfg(test)]
fn init () -> Vec<Update> {
    let mut ts : Vec<Update> = vec![];
    let t = Update {
        ts: 9,
        seq: 143,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    let t2 = Update {
        ts: 0,
        seq: 123,
        is_trade: true,
        is_bid: false,
        price: 5100.01,
        size: 1.123465,
    };
    ts.push(t);
    ts.push(t2);

    ts.sort();


    let fname = "test.bin".to_owned();
    let symbol = "NEO_BTC".to_owned();

    encode(&fname, &symbol, &ts);
    ts
}

#[test]
fn should_encode_and_decode_file() {
    let ts = init();
    let fname = "test.bin".to_owned();
    let decoded_updates = decode(&fname);
    assert_eq!(decoded_updates, ts);
}

#[test]
fn should_return_correct_symbol() {
    init();
    let fname = "test.bin".to_owned();
    let sym = get_symbol(&fname);
    assert_eq!(sym, "NEO_BTC ");
}