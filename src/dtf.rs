/// File format for Dense Tick Format (DTF)
/// 
/// 
/// File Spec:
/// Offset 00: ([u8; 5]) magic value 0x4454469001
/// Offset 05: ([u8; 9]) Symbol
/// Offset 14: (u64) number of records
/// Offset 21: (u32) max ts
/// Offset 80: -- records - see below --
/// 
/// 
/// Record Spec:
/// Offset 81: bool for is_snapshot
/// 1. if is true
///        4 bytes (u32): reference ts
///        2 bytes (u32): reference seq
///        2 bytes (u16): how many records between this snapshot and the next snapshot
/// 2. record
///        dts (u16): $ts - reference ts$, 2^16 = 65536 - ~65 seconds
///        dseq (u8) $seq - reference seq$ , 2^8 = 256
///        is_trade: (u8):
///        is_bid: (u8)
///        price: (f32)
///        size: (f32)

use conf;
use db;

use std::str;
use std::fs;
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
// static ITEM_OFFSET : u64 = 13; // each item has 13 bytes

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

    fn serialize(&self, ref_ts : u64, ref_seq : u32) -> Vec<u8> {
        let mut buf : Vec<u8> = Vec::new();
        let _ = buf.write_u16::<BigEndian>((self.ts- ref_ts) as u16);
        let _ = buf.write_u8((self.seq - ref_seq) as u8);
        let _ = buf.write_u8(self.is_trade as u8);
        let _ = buf.write_u8(self.is_bid as u8);
        let _ = buf.write_f32::<BigEndian>(self.price);
        let _ = buf.write_f32::<BigEndian>(self.size);
        buf
    }

    pub fn to_json(&self) -> String {
        format!(r#"{{"ts":{},"seq":{},"is_trade":{},"is_bid":{},"price":{},"size":{}}}"#,
                  (self.ts as f64) / 1000_f64, self.seq, self.is_trade, self.is_bid, self.price, self.size)
    }
}

impl Ord for Update {
    fn cmp(&self, other: &Update) -> Ordering {
        return self.partial_cmp(other).unwrap();
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

pub fn get_max_ts(updates : &[Update]) -> u64 {
    let mut max = 0;
    for update in updates.iter() {
        let current = update.ts;
        if current > max {
            max = current;
        }
    }
    max
}

fn file_writer(fname : &str, create : bool) -> BufWriter<File> {
    let new_file = if create {
        File::create(fname).unwrap()
    } else {
        fs::OpenOptions::new().write(true).open(fname).unwrap()
    };

    let wtr = BufWriter::new(new_file);
    wtr
}

fn write_magic_value(wtr: &mut BufWriter<File>) {
    let _ = wtr.write(MAGIC_VALUE);
}

fn write_symbol(wtr: &mut BufWriter<File>, symbol : &str) {
    assert!(symbol.len() <= SYMBOL_LEN);
    let padded_symbol = format!("{:width$}", symbol, width = SYMBOL_LEN); // right pad w/ space
    assert_eq!(padded_symbol.len(), SYMBOL_LEN);
    let _ = wtr.write(padded_symbol.as_bytes());
}

fn write_len(wtr: &mut BufWriter<File>, len : u64) {
    let _ = wtr.seek(SeekFrom::Start(LEN_OFFSET));
    wtr.write_u64::<BigEndian>(len).expect("length of records");
}

fn write_max_ts(wtr: &mut BufWriter<File>, max_ts : u64) {
    let _ = wtr.seek(SeekFrom::Start(MAX_TS_OFFSET));
    wtr.write_u64::<BigEndian>(max_ts).expect("maximum timestamp");
}

fn write_metadata(wtr: &mut BufWriter<File>, ups : &[Update]) {
    write_len(wtr, ups.len() as u64);
    write_max_ts(wtr, get_max_ts(ups));
}

fn write_reference(wtr: &mut Write, ref_ts: u64, ref_seq: u32, len: u16) {
    let _ = wtr.write_u8(true as u8);
    let _ = wtr.write_u64::<BigEndian>(ref_ts);
    let _ = wtr.write_u32::<BigEndian>(ref_seq);
    let _ = wtr.write_u16::<BigEndian>(len);
}

fn write_batches(mut wtr: &mut BufWriter<File>, ups : &[Update]) {
    let mut buf : Vec<u8> = Vec::new();
    let mut ref_ts = ups[0].ts;
    let mut ref_seq = ups[0].seq;
    let mut count = 0;

    for elem in ups.iter() {
        if count != 0 && elem.ts >= ref_ts + 65535 || elem.seq >= ref_seq + 255 {
            write_reference(&mut wtr, ref_ts, ref_seq, count);
            let _ = wtr.write(buf.as_slice());
            buf.clear();

            ref_ts = elem.ts;
            ref_seq = elem.seq;
            count = 0;
        }

        let serialized = elem.serialize(ref_ts, ref_seq);
        let _ = buf.write(serialized.as_slice());

        count += 1;
    }

    write_reference(&mut wtr, ref_ts, ref_seq, count);
    wtr.write(buf.as_slice()).unwrap();
}

fn write_main(wtr: &mut BufWriter<File>, ups : &[Update]) {
    let _ = wtr.seek(SeekFrom::Start(MAIN_OFFSET));
    if ups.len() > 0 {
        write_batches(wtr, ups);
    }
}

pub fn encode(fname : &str, symbol : &str, ups : &[Update]) {
    let mut wtr = file_writer(fname, true);

    write_magic_value(&mut wtr);
    write_symbol(&mut wtr, symbol);
    write_metadata(&mut wtr, ups);
    write_main(&mut wtr, ups);

    wtr.flush().expect("FAILURE TO FLUSH");
}

fn file_reader(fname: &str) -> BufReader<File> {

    let file = File::open(fname).expect("OPENING FILE");
    let mut rdr = BufReader::new(file);

    // magic value
    let _ = rdr.seek(SeekFrom::Start(0));
    let mut buf = vec![0u8; 5];
    let _ = rdr.read_exact(&mut buf);
    
    println!("{:?}", buf);

    if buf != MAGIC_VALUE {
        panic!("MAGIC VALUE INCORRECT");
    }

    rdr 
}

fn read_symbol(rdr : &mut BufReader<File>) -> String {
    rdr.seek(SeekFrom::Start(SYMBOL_OFFSET));

    let mut buffer = [0; SYMBOL_LEN];
    let _ = rdr.read_exact(&mut buffer);
    let symbol = str::from_utf8(&buffer).unwrap().to_owned();

    symbol
}

fn read_len(rdr : &mut BufReader<File>) -> u64 {
    rdr.seek(SeekFrom::Start(LEN_OFFSET)).unwrap();
    rdr.read_u64::<BigEndian>().expect("length of records")
}

fn read_min_ts(mut rdr: &mut BufReader<File>) -> u64 {
    read_first(&mut rdr).ts
}

fn read_max_ts(rdr : &mut BufReader<File>) -> u64 {
    let _ = rdr.seek(SeekFrom::Start(MAX_TS_OFFSET));
    rdr.read_u64::<BigEndian>().expect("maximum timestamp")
}

fn read_one_batch(rdr: &mut BufReader<File>) -> Vec<Update> {
    let is_ref = rdr.read_u8().expect("is_ref") == 0x00000001;
    let mut ref_ts = 0;
    let mut ref_seq = 0;
    let mut how_many = 0;
    let mut v : Vec<Update> = Vec::new();

    if is_ref {
        ref_ts = rdr.read_u64::<BigEndian>().unwrap();
        ref_seq = rdr.read_u32::<BigEndian>().unwrap();
        how_many = rdr.read_u16::<BigEndian>().unwrap();
        println!("WILL READ: COUNT {}", how_many);
    }

    for _i in 0..how_many {
        let current_update = Update {
            ts: rdr.read_u16::<BigEndian>().expect("ts") as u64 + ref_ts,
            seq: rdr.read_u8().expect("seq") as u32 + ref_seq,
            is_trade: rdr.read_u8().expect("is_trade") == 0x00000001,
            is_bid: rdr.read_u8().expect("is_bid") == 0x00000001,
            price: rdr.read_f32::<BigEndian>().expect("price"),
            size: rdr.read_f32::<BigEndian>().expect("size")
        };
        v.push(current_update);
    }

    v
}

fn read_first_batch(mut rdr: &mut BufReader<File>) -> Vec<Update> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    read_one_batch(&mut rdr)
}

fn read_first(mut rdr: &mut BufReader<File>) -> Update {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    let batch = read_one_batch(&mut rdr);
    batch[0].clone()
}

pub fn decode(fname: &str) -> Vec<Update> {
    let mut v : Vec<Update> = Vec::new();
    let mut rdr = file_reader(fname);
    let _symbol = read_symbol(&mut rdr); 
    let _nums = read_len(&mut rdr);
    let _max_ts = read_max_ts(&mut rdr);

    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

    while let Ok(is_ref) = rdr.read_u8() {
        if is_ref == 0x00000001 {
            rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
            v.extend(read_one_batch(&mut rdr));
        }
    }

    v
}

//TODO:
pub fn append(fname: &str, ups : &Vec<Update>) {

    let (ups, new_max_ts, cur_len) = {
        let mut rdr = file_reader(fname);
        let _symbol = read_symbol(&mut rdr);

        let old_max_ts = read_max_ts(&mut rdr);
        let _min_ts = read_min_ts(&mut rdr);

        let ups : Vec<Update> = ups.clone().into_iter()
                                    .filter(|up| up.ts > old_max_ts)
                                    .collect();

        let new_min_ts = ups[0].ts;
        let new_max_ts = ups[ups.len()-1].ts;

        if new_min_ts <= old_max_ts {
            panic!("Cannot append data!(not implemented)");
        }

        let cur_len = read_len(&mut rdr);
        (ups, new_max_ts, cur_len)
    };

    let new_len = cur_len + ups.len() as u64;

    let mut wtr = file_writer(fname, false);
    write_len(&mut wtr, new_len);
    write_max_ts(&mut wtr, new_max_ts);

    wtr.seek(SeekFrom::End(0)).unwrap();
    write_batches(&mut wtr, &ups);
    wtr.flush().unwrap();
}

#[cfg(test)]
fn sample_data() -> Vec<Update> {
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
        seq: 113,
        is_trade: true,
        is_bid: false,
        price: 5100.01,
        size: 1.123465,
    };
    ts.push(t);
    ts.push(t1);
    ts.push(t2);

    ts.sort();
    ts
}

#[cfg(test)]
fn sample_data_one_item() -> Vec<Update> {
    let mut ts : Vec<Update> = vec![];
    let t = Update {
        ts: 100,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    ts.push(t);

    ts.sort();
    ts
}


#[cfg(test)]
fn sample_data_append() -> Vec<Update> {
    let mut ts : Vec<Update> = vec![];
    let t2 = Update {
        ts: 00000002,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    let t1 = Update {
        ts: 20000001,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    let t = Update {
        ts: 20000000,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    ts.push(t);
    ts.push(t1);
    ts.push(t2);
    ts.sort();
    ts
}
#[cfg(test)]
fn init () -> Vec<Update> {
    let ts = sample_data();

    let fname = "test.dtf";
    let symbol = "NEO_BTC";

    encode(fname, symbol, &ts);

    ts
}

#[test]
fn should_encode_decode_one_item() {
    let ts = sample_data_one_item();
    let fname = "test.dtf";
    let symbol = "NEO_BTC";
    encode(fname, symbol, &ts);
    let decoded_updates = decode(fname);
    assert_eq!(decoded_updates, ts);
}

#[test]
fn should_encode_and_decode_file() {
    let ts = init();
    let fname = "test.dtf";
    let decoded_updates = decode(fname);
    assert_eq!(decoded_updates, ts);
}

#[test]
fn should_return_correct_symbol() {
    init();
    let fname = "test.dtf";
    let mut rdr = file_reader(fname);
    let sym = read_symbol(&mut rdr);
    assert_eq!(sym, "NEO_BTC  ");
}

#[test]
fn should_return_first_record() {
    let vs = init();
    let fname = "test.dtf";
    let mut rdr = file_reader(fname);
    let v = read_first(&mut rdr);
    assert_eq!(vs[0], v);
}

#[test]
fn should_return_correct_num_of_items() {
    let vs = init();
    let fname = "test.dtf";
    let mut rdr = file_reader(fname);
    let len = read_len(&mut rdr);
    assert_eq!(vs.len() as u64, len);
}

#[test]
fn should_return_max_ts() {
    let vs = init();
    let fname = "test.dtf";
    let mut rdr = file_reader(fname);
    let max_ts = read_max_ts(&mut rdr);
    assert_eq!(max_ts, get_max_ts(&vs));
}

// #[cfg(test)]
// fn init_real_data() -> Vec<Update> {
//     let conf = conf::get_config();
//     let cxn_str : &String = conf.get("connection_string").unwrap();
//     let updates : Vec<db::OrderBookUpdate> = db::run(&cxn_str);
//     let mut mapped : Vec<Update> = updates.iter().map(|d| d.to_update()).collect();
//     mapped.sort();
//     mapped
// }

// #[test]
// fn should_work_with_real_data() {
//     let mut vs = init_real_data();
//     let fname = "real.dtf";
//     let symbol = "NEO_BTC";
//     encode(fname, symbol, &mut vs);
//     let decoded_updates = decode(fname);
//     assert_eq!(decoded_updates, vs);
// }

#[test]
fn should_append_filtered_data() {
    should_encode_and_decode_file();

    println!("----DONE----");

    let fname = "test.dtf";
    let old_data = sample_data();
    let old_max_ts = get_max_ts(&old_data);
    let append_data : Vec<Update> = sample_data_append().into_iter().filter(|up| up.ts >= old_max_ts).collect();
    let new_size = append_data.len() + old_data.len();

    append(fname, &append_data);

    println!("----APPENDED----");

    let mut rdr = file_reader(fname);

    // max_ts
    let max_ts = read_max_ts(&mut rdr);
    assert_eq!(max_ts, get_max_ts(&append_data));

    // total len
    let mut rdr = file_reader(fname);
    let len = read_len(&mut rdr);
    assert_eq!(new_size as u64, len);

    let mut all_the_data = sample_data();
    all_the_data.extend(append_data);
    all_the_data.sort();
    let decoded = decode(&fname);
    assert_eq!(all_the_data, decoded);
    
}

#[test]
fn should_speak_json() {
    let t1 = Update {
        ts: 20000001,
        seq: 113,
        is_trade: false,
        is_bid: false,
        price: 5100.01,
        size: 1.14564564645,
    };
    assert_eq!(r#"{"ts":20000.001,"seq":113,"is_trade":false,"is_bid":false,"price":5100.01,"size":1.1456456}"#, t1.to_json());
}