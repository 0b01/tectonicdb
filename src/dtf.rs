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
//        dts (u16): $ts - reference ts$, 2^16 = 65536 - ~65 seconds
//        dseq (u8) $seq - reference seq$ , 2^8 = 256
//        is_trade: (u8):
//        is_bid: (u8)
//        price: (f32)
//        size: (f32)

use conf;
use db;

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
static ITEM_OFFSET : u64 = 13; // each item has 13 bytes

#[derive(Debug, Clone)]
pub struct Update {
    pub ts: u32,
    pub seq: u16,
    pub is_trade: bool,
    pub is_bid: bool,
    pub price: f32,
    pub size: f32,
}

impl Update {

    fn serialize_update(&self, ref_ts : u32, ref_seq : u16) -> Vec<u8> {
        let mut buf : Vec<u8> = Vec::new();
        buf.write_u16::<BigEndian>((self.ts - ref_ts) as u16).unwrap();
        buf.write_u8((self.seq - ref_seq) as u8).unwrap();
        buf.write_u8(self.is_trade as u8).unwrap();
        buf.write_u8(self.is_bid as u8).unwrap();
        buf.write_f32::<BigEndian>(self.price).unwrap();
        buf.write_f32::<BigEndian>(self.size).unwrap();
        buf
    }
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


pub fn get_max_ts(updates : &Vec<Update>) -> u32 {
    let mut max = 0;
    for update in updates.iter() {
        let current = update.ts;
        if current > max {
            max = current;
        }
    }
    max
}

fn file_writer(fname : &String) -> BufWriter<File> {
    let new_file = File::create(fname).unwrap();
    let mut wtr = BufWriter::new(new_file);
    wtr
}

fn write_magic_value(wtr: &mut BufWriter<File>) {
    wtr.write(MAGIC_VALUE).unwrap();
}


fn write_symbol(wtr: &mut BufWriter<File>, symbol : &String) {
    assert!(symbol.len() <= SYMBOL_LEN);
    let padded_symbol = format!("{:width$}", symbol, width = SYMBOL_LEN); // right pad w/ space
    assert!(padded_symbol.len() == SYMBOL_LEN);
    wtr.write(padded_symbol.as_bytes()).unwrap();
}

fn write_metadata(wtr: &mut BufWriter<File>, ups : &Vec<Update>) {
    // number of records
    wtr.write_u64::<BigEndian>(ups.len() as u64).expect("length of records");

    // max ts
    let max_ts = get_max_ts(&ups);
    wtr.write_u32::<BigEndian>(max_ts).expect("maximum timestamp");
}

fn write_reference(wtr: &mut Write, ref_ts: u32, ref_seq: u16, len: u16) {
    wtr.write_u8(true as u8).unwrap();
    wtr.write_u32::<BigEndian>(ref_ts).unwrap();
    wtr.write_u16::<BigEndian>(ref_seq).unwrap();
    wtr.write_u16::<BigEndian>(len).unwrap();
}

fn write_main(mut wtr: &mut BufWriter<File>, ups : &Vec<Update>) {
    wtr.seek(SeekFrom::Start(MAIN_OFFSET)).unwrap();

    let mut buf : Vec<u8> = Vec::new();

    let mut ref_ts = ups[0].ts;
    let mut ref_seq = ups[0].seq;
    let mut count = 0;

    for elem in ups.iter() {
        if count != 0 && elem.ts >= ref_ts + 65535 || elem.seq >= ref_seq + 255 {
            write_reference(&mut wtr, ref_ts, ref_seq, count);
            wtr.write(buf.as_slice()).unwrap();
            buf.clear();

            ref_ts = elem.ts;
            ref_seq = elem.seq;
            count = 0;
        }

        let serialized = elem.serialize_update(ref_ts, ref_seq);
        buf.write_u8(false as u8).unwrap();
        buf.write(serialized.as_slice()).unwrap();

        count += 1;
    }

    write_reference(&mut wtr, ref_ts, ref_seq, count);
    wtr.write(buf.as_slice()).unwrap();
}

pub fn encode(fname : &String, symbol : &String, ups : &mut Vec<Update>) {
    ups.sort();
    let mut wtr = file_writer(&fname);

    write_magic_value(&mut wtr);
    write_symbol(&mut wtr, &symbol);
    write_metadata(&mut wtr, &ups);
    write_main(&mut wtr, &ups);

    wtr.flush().expect("FAILURE TO FLUSH");
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

pub fn read_min_ts(mut rdr: &mut BufReader<File>) -> u32 {
    read_first(&mut rdr).ts
}

pub fn read_max_ts(rdr : &mut BufReader<File>) -> u32 {
    rdr.seek(SeekFrom::Start(MAX_TS_OFFSET));
    rdr.read_u32::<BigEndian>().expect("maximum timestamp")
}

pub fn read_one_batch(rdr: &mut BufReader<File>) -> Vec<Update> {
    let is_ref = rdr.read_u8().expect("is_ref") == 0x00000001;
    let mut ref_ts = 0;
    let mut ref_seq = 0;
    let mut how_many = 0;
    let mut v : Vec<Update> = Vec::new();

    if is_ref {
        ref_ts = rdr.read_u32::<BigEndian>().unwrap();
        ref_seq = rdr.read_u16::<BigEndian>().unwrap();
        how_many = rdr.read_u16::<BigEndian>().unwrap();
        println!("WILL READ: COUNT {}", how_many);
    }

    for _i in 0..how_many {
        assert!(rdr.read_u8().expect("is_ref") == 0x00000000);
        let current_update = Update {
            ts: rdr.read_u16::<BigEndian>().expect("ts") as u32 + ref_ts,
            seq: rdr.read_u8().expect("seq") as u16 + ref_seq,
            is_trade: rdr.read_u8().expect("is_trade") == 0x00000001,
            is_bid: rdr.read_u8().expect("is_bid") == 0x00000001,
            price: rdr.read_f32::<BigEndian>().expect("price"),
            size: rdr.read_f32::<BigEndian>().expect("size")
        };
        v.push(current_update);
    }

    v
}

pub fn read_first_batch(mut rdr: &mut BufReader<File>) -> Vec<Update> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    read_one_batch(&mut rdr)
}

pub fn read_first(mut rdr: &mut BufReader<File>) -> Update {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    let batch = read_one_batch(&mut rdr);
    batch[0].clone()
}

pub fn decode(fname: &String) -> Vec<Update> {
    let mut v : Vec<Update> = Vec::new();
    let mut rdr = file_reader(&fname);
    let _symbol = read_symbol(&mut rdr); 
    let _nums = read_len(&mut rdr);
    let _max_ts = read_max_ts(&mut rdr);

    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

    loop {
        match rdr.read_u8() {
            Ok(is_ref) => {
                if is_ref == 0x00000001 {
                    rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
                    v.extend(read_one_batch(&mut rdr));
                }
            }
            Err(_err) => {
                break;
            }
        }
    }
    v
}

#[cfg(test)]
fn init () -> Vec<Update> {
    let mut ts : Vec<Update> = vec![];
    let t = Update {
        ts: 100,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    let t1 = Update {
        ts: 101,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 2.14564564645,
    };
    let t2 = Update {
        ts: 1000000,
        seq: 123,
        is_trade: true,
        is_bid: false,
        price: 5100.01,
        size: 1.123465,
    };
    ts.push(t);
    ts.push(t1);
    ts.push(t2);

    ts.sort();


    let fname = "test.bin".to_owned();
    let symbol = "NEO_BTC".to_owned();

    encode(&fname, &symbol, &mut ts);

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

#[cfg(test)]
fn init_real_data() -> Vec<Update> {
    let conf = conf::get_config();
    let cxn_str : &String = conf.get("connection_string").unwrap();

    let updates : Vec<db::OrderBookUpdate> = db::run(&cxn_str);
    let mut mapped = updates.iter().map(|d| d.to_update()).collect();
    mapped
}

#[test]
fn should_work_with_real_data() {
    let mut vs = init_real_data();

    let fname = "test.bin".to_owned();
    let symbol = "NEO_BTC".to_owned();
    encode(&fname, &symbol, &mut vs);
    let decoded_updates = decode(&fname);
    assert_eq!(decoded_updates, vs);
}

#[test]
fn should_append() {
}