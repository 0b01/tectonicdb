// File format for Dense Tick Format (DTF)

// File Type:
// Offset 00: ([u8; 5]) magic value 0x4454469001
// Offset 05: ([u8; 9]) Symbol
// Offset 14: (u64) number of records
// Offset 21: (u32) max ts
// Offset 80: -- records - see below --

// Record Spec:
// Offset 81: bool for is_snapshot
// 1. if is snapshot
//        4 bytes (u32): reference ts
//        2 bytes (u16): reference seq
//        2 bytes (u16): how many records between this snapshot and the next snapshot
//        
// 2. if is record
//        dts (u8): $ts - reference ts$
//        dseq (u8): $seq - reference seq$ 
//        is_trade: (u8):
//        is_bid: (u8)
//        price: (f32)
//        size: (f32)

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
const SYMBOL_LEN : usize = 9;
static SYMBOL_OFFSET : u64 = 5;
static LEN_OFFSET : u64 = 14;
static MAX_TS_OFFSET : u64 = 22;
static MAIN_OFFSET : u64 = 80; // main section start at 80
static ITEM_OFFSET : u64 = 22; // each item has 22 bytes

#[derive(Debug)]
pub struct Update {
    pub ts: u64,
    pub seq: u32,
    pub is_trade: bool,
    pub is_bid: bool,
    pub price: f32,
    pub size: f32,
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

pub fn get_max_ts(updates : &Vec<Update>) -> u64 {
    let mut max = 0;
    for update in updates.iter() {
        let current = update.ts;
        if current > max {
            max = current;
        }
    }
    max
}

pub fn encode(fname : &String, symbol : &String, ups : &Vec<Update>) {
    let new_file = File::create(fname).unwrap();
    let mut writer = BufWriter::new(new_file);
    // headers
    writer.write(MAGIC_VALUE).unwrap();

    // write symbol
    assert!(symbol.len() <= SYMBOL_LEN);
    let padded_symbol = format!("{:9}", symbol);
    assert!(padded_symbol.len() == SYMBOL_LEN);
    writer.write(padded_symbol.as_bytes()).unwrap();

    // number of records
    writer.write_u64::<BigEndian>(ups.len() as u64).expect("length of records");

    // max ts
    let max_ts = get_max_ts(&ups);
    writer.write_u64::<BigEndian>(max_ts).expect("maximum timestamp");

    writer.seek(SeekFrom::Start(MAIN_OFFSET)).unwrap();
    for elem in ups.into_iter() {
        let serialized = serialize_update(&elem);
        writer.write(serialized.as_slice()).unwrap();
    }
    writer.flush().expect("FAILURE TO FLUSH");
}

//TODO:
pub fn append(fname: &String, ups : &mut Vec<Update>) {
    let new_max = {
        let mut rdr = file_reader(&fname);
        let _symbol = read_symbol(&mut rdr);

        let max_ts = read_max_ts(&mut rdr);
        let max_ts = read_min_ts(&mut rdr);

        ups.sort();
        let new_min = ups[0].ts;
        let new_max = ups[ups.len()-1].ts;

        if new_min <= max_ts {
            panic!("Cannot append data!(not implemented)");
        }
        new_max
    };





}

// parsers:

fn file_reader(fname: &String) -> BufReader<File> {

    let file = File::open(fname).expect("OPENING FILE");
    let mut rdr = BufReader::new(file);

    // magic value
    rdr.seek(SeekFrom::Start(0));
    let mut buf = vec![0u8; 5];
    rdr.read_exact(&mut buf).unwrap();
    if buf != MAGIC_VALUE {
        panic!("MAGIC VALUE INCORRECT");
    }

    rdr 
}
pub fn read_symbol(rdr : &mut BufReader<File>) -> String {
    rdr.seek(SeekFrom::Start(SYMBOL_OFFSET));

    // read symbol
    let mut buffer = [0; SYMBOL_LEN];
    rdr.read_exact(&mut buffer).unwrap();
    let symbol = str::from_utf8(&buffer).unwrap().to_owned();

    symbol
}

pub fn read_len(rdr : &mut BufReader<File>) -> u64 {
    rdr.seek(SeekFrom::Start(LEN_OFFSET));
    rdr.read_u64::<BigEndian>().expect("length of records")
}

pub fn read_min_ts(mut rdr: &mut BufReader<File>) -> u64 {
    read_first(&mut rdr).ts
}

pub fn read_max_ts(rdr : &mut BufReader<File>) -> u64 {
    rdr.seek(SeekFrom::Start(MAX_TS_OFFSET));
    rdr.read_u64::<BigEndian>().expect("maximum timestamp")
}

pub fn read_one(rdr: &mut BufReader<File>) -> Update {

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

pub fn read_first(mut rdr: &mut BufReader<File>) -> Update {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    read_one(&mut rdr)
}

pub fn decode(fname: &String) -> Vec<Update> {
    let mut v : Vec<Update> = Vec::new();
    let mut rdr = file_reader(&fname);
    let _symbol = read_symbol(&mut rdr); 
    let nums = read_len(&mut rdr);
    let _max_ts = read_max_ts(&mut rdr);
    for n in 0..nums {
        rdr.seek(SeekFrom::Start(MAIN_OFFSET + n * ITEM_OFFSET)).expect("SEEKING");
        v.push(read_one(&mut rdr));
    }

    v
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
    let mut rdr = file_reader(&fname);
    let sym = read_symbol(&mut rdr);
    assert_eq!(sym, "NEO_BTC  ");
}

#[test]
fn should_return_first_record() {
    let vs = init();
    let fname = "test.bin".to_owned();
    let mut rdr = file_reader(&fname);
    let v = read_first(&mut rdr);
    assert_eq!(vs[0], v);
}

#[test]
fn should_return_correct_num_of_items() {
    let vs = init();
    let fname = "test.bin".to_owned();
    let mut rdr = file_reader(&fname);
    let len = read_len(&mut rdr);
    assert_eq!(vs.len() as u64, len);
}

#[test]
fn should_return_max_ts() {
    let vs = init();
    let fname = "test.bin".to_owned();
    let mut rdr = file_reader(&fname);
    let max_ts = read_max_ts(&mut rdr);
    assert_eq!(max_ts, get_max_ts(&vs));
}