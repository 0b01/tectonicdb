/// 
/// File format for Dense Tick Format (DTF)
/// 
/// 
/// File Spec:
/// Offset 00: ([u8; 5]) magic value 0x4454469001
/// Offset 05: ([u8; 20]) Symbol
/// Offset 25: (u64) number of records
/// Offset 33: (u32) max ts
/// Offset 80: -- records - see below --
/// 
/// 
/// Record Spec:
/// Offset 81: bool for `is_snapshot`
/// 1. if is true
///        4 bytes (u32): reference ts
///        2 bytes (u32): reference seq
///        2 bytes (u16): how many records between this snapshot and the next snapshot
/// 2. record
///        dts (u16): $ts - reference ts$, 2^16 = 65536 - ~65 seconds
///        dseq (u8) $seq - reference seq$ , 2^8 = 256
///        `is_trade & is_bid`: (u8): bitwise and to store two bools in one byte
///        price: (f32)
///        size: (f32)



use update::*;
use std::str;
use std::fs;
use std::fs::File;
use std::fmt;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::io::{
    Write,
    Read,
    Seek,
    BufWriter,
    BufReader,
    SeekFrom
};

static MAGIC_VALUE : &[u8] = &[0x44, 0x54, 0x46, 0x90, 0x01]; // DTF9001
const SYMBOL_LEN : usize = 20;
static SYMBOL_OFFSET : u64 = 5;
static LEN_OFFSET : u64 = 25;
static MAX_TS_OFFSET : u64 = 33;
static MAIN_OFFSET : u64 = 80; // main section start at 80
// static ITEM_OFFSET : u64 = 13; // each item has 13 bytes


pub struct Metadata {
    pub symbol: String,
    pub nums: u64,
    pub max_ts: u64,
    pub min_ts: u64
}

#[derive(Clone)]
pub struct BatchMetadata {
    pub ref_ts: u64,
    pub ref_seq: u32,
    pub count: u16
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, r#"{{
  "symbol": "{}",
  "nums": {},
  "max_ts": {},
  "min_ts": {}
}}"#,
    self.symbol,
    self.nums,
    self.max_ts,
    self.min_ts 
    )
    }
}


pub fn update_vec_to_csv(vecs: &[Update]) -> String {
    let objects : Vec<String> = vecs.into_iter().map(|up| up.to_csv()).collect();
    objects.join("\n")
}

pub fn update_vec_to_json(vecs: &[Update]) -> String {
    let objects : Vec<String> = vecs.into_iter().map(|up| up.to_json()).collect();
    objects.join(", ")
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

    BufWriter::new(new_file)
}

fn write_magic_value(wtr: &mut Write) {
    let _ = wtr.write(MAGIC_VALUE);
}

fn write_symbol(wtr: &mut Write, symbol : &str) {
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

pub fn write_batches(mut wtr: &mut Write, ups : &[Update]) {
    let mut buf : Vec<u8> = Vec::new();
    let mut ref_ts = ups[0].ts;
    let mut ref_seq = ups[0].seq;
    let mut count = 0;

    for elem in ups.iter() {
        if count != 0 // if we got things to write
        && (
             elem.ts >= ref_ts + 0xFFFF // if still addressable (ref_ts is 4 bytes)
          || elem.seq >= ref_seq + 0xF // ref_seq is 1 byte
          || elem.seq < ref_seq // sometimes the data is scrambled, just write that line down
          || elem.ts < ref_ts // ^
         ) {
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
    wtr.write_all(buf.as_slice()).unwrap();
}

fn write_main(wtr: &mut BufWriter<File>, ups : &[Update]) {
    let _ = wtr.seek(SeekFrom::Start(MAIN_OFFSET));
    if !ups.is_empty() {
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

    if buf != MAGIC_VALUE {
        panic!("MAGIC VALUE INCORRECT");
    }

    rdr 
}

fn read_symbol(rdr : &mut BufReader<File>) -> String {
    rdr.seek(SeekFrom::Start(SYMBOL_OFFSET)).unwrap();
    let mut buffer = [0; SYMBOL_LEN];
    let _ = rdr.read_exact(&mut buffer);
    str::from_utf8(&buffer).unwrap().to_owned()
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

pub fn read_one_batch_meta(rdr: &mut Read) -> BatchMetadata {
    let ref_ts = rdr.read_u64::<BigEndian>().unwrap();
    let ref_seq = rdr.read_u32::<BigEndian>().unwrap();
    let count = rdr.read_u16::<BigEndian>().unwrap();

    BatchMetadata {
        ref_ts,
        ref_seq,
        count
    }
}

/// reads a vector of Update over some time interval (min_ts, max_ts) from file.
pub fn range(rdr: &mut BufReader<File>, min_ts: f64, max_ts: f64) -> Vec<Update> {
    // convert ts to match the dtf file format (in ms)
    let min_ts = (min_ts * 1000.) as u64;
    let max_ts = (max_ts * 1000.) as u64;

    // can't go back in time
    if min_ts > max_ts { return Vec::new(); }
    // go to beginning of main section
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    let mut v : Vec<Update> = Vec::new();

    loop {
        // read marker byte
        match rdr.read_u8() {
            Ok(byte) => { if byte != 0x1 { return v; } },   // 0x1 indicates a batch
            Err(_e) => { return v; }                        // EOF
        };

        // read the metadata of the current batch
        let current_meta = read_one_batch_meta(rdr);
        let current_ref_ts = current_meta.ref_ts;
        let current_count = current_meta.count;

        // skip a few bytes and read the next metadata
        let bytes_to_skip = current_count * 12 /* 12 bytes per row */;
        rdr.seek(SeekFrom::Current(bytes_to_skip as i64)).expect(&format!("Skipping {} rows", current_count));

        // must be a batch
        match rdr.read_u8() {
            Ok(byte) => { if byte != 0x1 { return v; } },   // is a batch
            Err(_e) => { return v; }                        // EOF
        };
        let next_meta = read_one_batch_meta(rdr);
        let next_ref_ts = next_meta.ref_ts;

        // legend:
        // `|`: meta data
        // `1`: indicator byte
        // `-`: updates

        //     |1-----|1*---      <- we are here

        //  [ ]                   <- requested
        //     |1-----|1---
        // 
        if min_ts <= current_ref_ts && max_ts <= current_ref_ts {
            return v;
        } else

        // [    ]
        //   |1*-----|1---
        //
        // or 
        //
        //         [     ]
        //         [          ]
        //   |1*-----|1----|1---
        //
        if (min_ts <= current_ref_ts && max_ts <= next_ref_ts )
            || (min_ts < next_ref_ts && max_ts >= next_ref_ts) {
            // seek back
            let bytes_to_scrollback = - (bytes_to_skip as i64) - 14 /* metadata */ - 1 /* indicator byte */ ;
            rdr.seek(SeekFrom::Current( bytes_to_scrollback )).expect("scrolling back");
            //   |1*------|1--          <- we are here
            // read and filter current batch
            let filtered = {
                let batch = read_one_batch_main(rdr, current_meta);
                if min_ts <= current_ref_ts && max_ts >= next_ref_ts {
                    batch
                } else {
                    batch.into_iter()
                        .filter(|up| up.ts <= max_ts && up.ts >= min_ts)
                        .collect::<Vec<Update>>()
                }
            };
            v.extend(filtered);
        
        //               [      ]
        // |1----|1---|1----
        //
        } else if min_ts >= next_ref_ts {
            // simply skip back to the beginning of the second batch
            // |1----*|1---|1---
            let bytes_to_scrollback = - 14 /* metadata */ - 1 /* indicator byte */ ;
            rdr.seek(SeekFrom::Current( bytes_to_scrollback )).expect("SKIPPING n ROWS");
        } else {
            panic!("Should have cover all the cases.");
        }
    }
}

pub fn read_one_batch(rdr: &mut Read) -> Vec<Update> {
    let is_ref = rdr.read_u8().expect("is_ref") == 0x1;
    if !is_ref {
        Vec::new()
    } else {
        let meta = read_one_batch_meta(rdr);
        read_one_batch_main(rdr, meta)
    }
}

fn read_one_batch_main(rdr: &mut Read, meta: BatchMetadata) -> Vec<Update> {
    let mut v : Vec<Update> = Vec::new();
    for _i in 0..meta.count {
        let up = read_one_update(rdr, &meta);
        v.push(up);
    }
    v
}

fn read_one_update(rdr: &mut Read, meta: &BatchMetadata) -> Update {
    let ts = u64::from(rdr.read_u16::<BigEndian>().expect("ts")) + meta.ref_ts;
    let seq = u32::from(rdr.read_u8().expect("seq")) + meta.ref_seq;
    let flags = rdr.read_u8().expect("is_trade and is_bid");
    let is_trade = (Flags::from_bits(flags).unwrap() & Flags::FLAG_IS_TRADE).to_bool();
    let is_bid = (Flags::from_bits(flags).unwrap() & Flags::FLAG_IS_BID).to_bool();
    let price = rdr.read_f32::<BigEndian>().expect("price");
    let size = rdr.read_f32::<BigEndian>().expect("size");
    Update {
        ts, seq, is_trade, is_bid, price, size
    }
}

fn read_first_batch(mut rdr: &mut BufReader<File>) -> Vec<Update> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    read_one_batch(&mut rdr)
}

fn read_first(mut rdr: &mut BufReader<File>) -> Update {
    let batch = read_first_batch(&mut rdr);
    batch[0].clone()
}

pub fn get_size(fname: &str) -> u64 {
    let mut rdr = file_reader(fname);
    read_len(&mut rdr)
}

pub fn read_meta(fname: &str) -> Metadata {
    let mut rdr = file_reader(fname);
    let symbol = read_symbol(&mut rdr); 
    let nums = read_len(&mut rdr);
    let max_ts = read_max_ts(&mut rdr);
    let min_ts = read_min_ts(&mut rdr);

    Metadata{
        symbol,
        nums,
        max_ts,
        min_ts
    }

}

/// decode main section
pub fn decode(fname: &str, num_rows : Option<u32>) -> Vec<Update> {
    let mut v : Vec<Update> = Vec::new();

    let mut rdr = file_reader(fname);
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

    match num_rows {
        Some(num_rows) => {
            let mut count = 0;
            while let Ok(is_ref) = rdr.read_u8() {
                if count > num_rows { break; }

                if is_ref == 0x1 {
                    rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
                    v.extend(read_one_batch(&mut rdr));
                }

                count += 1;
            }
        },
        None => {
            while let Ok(is_ref) = rdr.read_u8() {
                if is_ref == 0x1 {
                    rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
                    v.extend(read_one_batch(&mut rdr));
                }
            }
        }
    }


    v
}

pub fn append(fname: &str, ups : &[Update]) {

    let (ups, new_max_ts, cur_len) = {
        let mut rdr = file_reader(fname);
        let _symbol = read_symbol(&mut rdr);

        let old_max_ts = read_max_ts(&mut rdr);

        let ups : Vec<Update> = ups.into_iter()
                                    .filter(|up| up.ts > old_max_ts)
                                    .cloned()
                                    .collect();
        if ups.is_empty() {
            return;
        }

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

    if cur_len == 0 {
        wtr.seek(SeekFrom::Start(MAIN_OFFSET)).unwrap();
    } else {
        wtr.seek(SeekFrom::End(0)).unwrap();
    }
    write_batches(&mut wtr, &ups);
    wtr.flush().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn init () -> Vec<Update> {
        let ts = sample_data();

        let fname = "test.dtf";
        let symbol = "NEO_BTC";

        encode(fname, symbol, &ts);

        ts
    }

    #[test]
    fn should_format_metadata_properly() {
        let meta = Metadata { 
            symbol: "TEST".to_owned(), 
            nums: 1, 
            max_ts: 1, 
            min_ts: 1 
        };

        assert_eq!(format!("{}", meta), r#"{
  "symbol": "TEST",
  "nums": 1,
  "max_ts": 1,
  "min_ts": 1
}"#);
    }

    #[test]
    fn should_encode_decode_one_item() {
        let ts = sample_data_one_item();
        let fname = "test.dtf";
        let symbol = "NEO_BTC";
        encode(fname, symbol, &ts);
        let decoded_updates = decode(fname, None);
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    fn should_encode_and_decode_file() {
        let ts = init();
        let fname = "test.dtf";
        let decoded_updates = decode(fname, None);
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    fn should_return_the_correct_range() {
        let fname = "test.dtf";
        {
            // let wtr = file_writer(fname, true);
            let ups =
                (1..1000).map(|i| 
                    Update {
                        ts: i*1000 as u64,
                        seq: i as u32,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false
                    })
                .collect::<Vec<Update>>();

            encode(fname, "test", &ups);
        }
        
        let mut rdr = file_reader(fname);
        assert_eq!((10..21).map(|i| 
                    Update {
                        ts: i*1000 as u64,
                        seq: i as u32,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false
                    })
                .collect::<Vec<Update>>(), range(&mut rdr, 10., 20.));
    }

    #[test]
    fn should_return_the_correct_range_2() {
        let fname = "test.dtf";
        {
            // let wtr = file_writer(fname, true);
            let ups =
                (1..1000).map(|i| 
                    Update {
                        ts: i*1000 as u64,
                        seq: i as u32 % 500 * 500,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false
                    })
                .collect::<Vec<Update>>();

            encode(fname, "test", &ups);
        }
        
        let mut rdr = file_reader(fname);
        assert_eq!((1..999).map(|i| 
                    Update {
                        ts: i*1000 as u64,
                        seq: i as u32 % 500 * 500,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false
                    })
                .collect::<Vec<Update>>(), range(&mut rdr, 1., 999.)); // ???
    }

    #[test]
    fn should_return_correct_range_real() {
        let fname: &str = "test-data/bt_btcnav.dtf";
        let mut rdr = file_reader(fname);

        let start = 1_510_168_156.;
        let end = 1_510_171_756.;

        let ups = range(&mut rdr, start, end);
        println!("{}", ups.len());
        assert_eq!(ups.len(), 10736);

        for up in ups.iter() {
            assert!(up.ts >= (start * 1000.) as u64 && up.ts <= (end * 1000.) as u64);
        }
    }

    // TODO: write more test cases...

    #[test]
    fn should_return_correct_symbol() {
        init();
        let fname = "test.dtf";
        let mut rdr = file_reader(fname);
        let sym = read_symbol(&mut rdr);
        assert_eq!(sym, "NEO_BTC             ");
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
    //     use conf;
    //     use db;
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
    //     let decoded_updates = decode(fname, None);
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
        let decoded = decode(&fname, None);
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
}