//!
//! File format for Dense Tick Format (DTF)
//!
//!
//! File Spec:
//! Offset 00: ([u8; 5]) magic value 0x4454469001
//! Offset 05: ([u8; 20]) Symbol
//! Offset 25: (u64) number of records
//! Offset 33: (u64) max ts
//! Offset 80: -- records - see below --
//!
//!
//! Record Spec:
//! Offset 81: bool for `is_snapshot`
//! 1. if is true
//!        4 bytes (u32): reference ts
//!        2 bytes (u32): reference seq
//!        2 bytes (u16): how many records between this snapshot and the next snapshot
//! 2. record
//!        dts (u16): $ts - reference ts$, 2^16 = 65536 - ~65 seconds
//!        dseq (u8) $seq - reference seq$ , 2^8 = 256
//!        `is_trade & is_bid`: (u8): bitwise and to store two bools in one byte
//!        price: (f32)
//!        size: (f32)

const BYTES_PER_ROW: usize = 12;

#[cfg(feature = "count_alloc")]
use alloc_counter::{count_alloc, no_alloc};

use std::str;
use std::fs;
use std::fs::File;
use std::fmt;
use std::cmp;
use std::io::ErrorKind::InvalidData;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::io::{self, Write, Read, Seek, BufWriter, BufReader, SeekFrom};

use std::iter::Peekable;
use std::io::Cursor;
use std::sync::Mutex;
use std::cell::RefCell;
use std::ops::DerefMut;

use crate::dtf::update::*;
use crate::utils::epoch_to_human;

static MAGIC_VALUE: &[u8] = &[0x44, 0x54, 0x46, 0x90, 0x01]; // DTF9001
const SYMBOL_LEN: usize = 20;
static SYMBOL_OFFSET: u64 = 5;
static LEN_OFFSET: u64 = 25;
static MAX_TS_OFFSET: u64 = 33;
static MAIN_OFFSET: u64 = 80; // main section start at 80
// static ITEM_OFFSET : u64 = 13; // each item has 13 bytes

/// Metadata block, one per file
#[derive(Debug, Eq, PartialEq, PartialOrd)]
pub struct Metadata {
    /// Symbol name
    pub symbol: String,
    /// Number of `Update`s in the file
    pub count: u64,
    /// The timestamp of the last item
    pub max_ts: u64,
    /// The smallest timestamp
    pub min_ts: u64,
}


impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        u64::cmp(&self.min_ts, &other.min_ts)
    }
}

/// Metadata block for each Batch
#[derive(Clone, Debug)]
pub struct BatchMetadata {
    /// reference timestamp
    pub ref_ts: u64,
    /// reference seq
    pub ref_seq: u32,
    /// count of updates in the block that follows
    pub count: u16,
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            r#"{{
  "symbol": "{}",
  "count": {},
  "max_ts": {},
  "max_ts_human": "{}",
  "min_ts": {},
  "min_ts_human": "{}"
}}"#,
            self.symbol,
            self.count,
            self.max_ts,
            epoch_to_human(self.max_ts / 1000),
            self.min_ts,
            epoch_to_human(self.min_ts / 1000)
        )
    }
}

/// Get max timestamp from a slice of sorted updates
pub fn get_max_ts_sorted(updates: &[Update]) -> u64 {
    updates.last().unwrap().ts
}

fn file_writer(fname: &str, create: bool) -> Result<BufWriter<File>, io::Error> {
    let new_file = if create {
        File::create(fname)?
    } else {
        fs::OpenOptions::new().write(true).open(fname)?
    };

    Ok(BufWriter::new(new_file))
}

fn write_magic_value(wtr: &mut dyn Write) -> Result<usize, io::Error> {
    wtr.write(MAGIC_VALUE)
}

fn write_symbol(wtr: &mut dyn Write, symbol: &str) -> Result<usize, io::Error> {
    if symbol.len() > SYMBOL_LEN {
        return Err(io::Error::new(InvalidData,
            format!("Symbol length is longer than {}", SYMBOL_LEN)));
    }
    let padded_symbol = format!("{:width$}", symbol, width = SYMBOL_LEN); // right pad w/ space
    assert_eq!(padded_symbol.len(), SYMBOL_LEN);
    wtr.write(padded_symbol.as_bytes())
}

fn write_len<T: Write + Seek>(wtr: &mut T, len: u64) -> Result<(), io::Error> {
    let _ = wtr.seek(SeekFrom::Start(LEN_OFFSET));
    wtr.write_u64::<BigEndian>(len)
}

fn write_max_ts<T: Write + Seek>(wtr: &mut T, max_ts: u64) -> Result<(), io::Error> {
    let _ = wtr.seek(SeekFrom::Start(MAX_TS_OFFSET));
    wtr.write_u64::<BigEndian>(max_ts)
}

fn write_metadata<T: Write + Seek>(wtr: &mut T, ups: &[Update]) -> Result<(), io::Error> {
    write_len(wtr, ups.len() as u64)?;
    write_max_ts(wtr, get_max_ts_sorted(ups))
}

fn write_reference(wtr: &mut dyn Write, ref_ts: u64, ref_seq: u32, len: u16) -> Result<(), io::Error> {
    wtr.write_u8(true as u8)?;
    wtr.write_u64::<BigEndian>(ref_ts)?;
    wtr.write_u32::<BigEndian>(ref_seq)?;
    wtr.write_u16::<BigEndian>(len)
}

/// write a list of updates as batches
#[cfg_attr(feature="count_alloc", count_alloc)]
pub fn write_batches<'a, I: Iterator<Item=&'a Update>>(mut wtr: &mut dyn Write, mut ups: Peekable<I>) -> Result<(), io::Error> {
    lazy_static! {
        static ref BUF: Mutex<RefCell<Vec<u8>>> = Mutex::new(RefCell::new(vec![0; 100_000_000]));
    }
    let mut b = BUF.lock().unwrap();
    let mut c = b.deref_mut().borrow_mut();
    let mut buf = Cursor::new(&mut c[..]);
    // let mut buf = Vec::new();
    let head = ups.peek().unwrap();
    let mut ref_ts = head.ts;
    let mut ref_seq = head.seq;
    let mut count: u16 = 0;

    for elem in ups {
        if count != 0 // if we got things to write
        && (
             elem.ts >= ref_ts + 0xFFFF // if still addressable (ref_ts is 4 bytes)
          || elem.seq >= ref_seq + 0xF // ref_seq is 1 byte
          || elem.seq < ref_seq // sometimes the data is scrambled, just write that line down
          || elem.ts < ref_ts // ^
          || count == 0xFFFF
         )
        {
            write_reference(&mut wtr, ref_ts, ref_seq, count)?;
            let _ = wtr.write(&buf.get_ref()[0..(buf.position() as usize)]);
            buf.set_position(0);
            // let _ = wtr.write(buf.as_slice());
            // buf.clear();

            ref_ts = elem.ts;
            ref_seq = elem.seq;
            count = 0;
        }

        elem.serialize_to_buffer(&mut buf, ref_ts, ref_seq);

        count += 1;
    }

    write_reference(&mut wtr, ref_ts, ref_seq, count)?;
    wtr.write_all(&buf.get_ref()[0..(buf.position() as usize)])
    // wtr.write_all(buf.as_slice())
}

fn write_main<'a, T: Write + Seek, I: IntoIterator<Item=&'a Update>>(wtr: &mut T, ups: I) -> Result<(), io::Error> {
    wtr.seek(SeekFrom::Start(MAIN_OFFSET))?;
    write_batches(wtr, ups.into_iter().peekable())?;
    Ok(())
}

/// write a list of updates to file
pub fn encode(fname: &str, symbol: &str, ups: &[Update]) -> Result<(), io::Error> {
    let mut wtr = file_writer(fname, true)?;
    encode_buffer(&mut wtr, symbol, ups)?;
    wtr.flush()
}

/// encode file format into a buffer
/// complete w ith magic value, symbol, metadata
pub fn encode_buffer<T: Write + Seek>(wtr: &mut T, symbol: &str, ups: &[Update]) -> Result<(), io::Error> {
    if !ups.is_empty() {
        write_magic_value(wtr)?;
        write_symbol(wtr, symbol)?;
        write_metadata(wtr, ups)?;
        write_main(wtr, ups)?;
    }
    Ok(())
}

/// check magic value
pub fn is_dtf(fname: &str) -> Result<bool, io::Error> {
    let file = File::open(fname)?;
    let mut rdr = BufReader::new(file);
    read_magic_value(&mut rdr)
}

/// reads magic value from buffer and checks it
pub fn read_magic_value<T: Read + Seek>(rdr: &mut T) -> Result<bool, io::Error> {
    // magic value
    rdr.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; 5];
    rdr.read_exact(&mut buf)?;
    Ok(buf == MAGIC_VALUE)
}

/// BufReader for dtf file
/// returns Error if not a dtf file
pub fn file_reader(fname: &str) -> Result<BufReader<File>, io::Error> {
    let file = File::open(fname)?;
    let mut rdr = BufReader::new(file);

    if !read_magic_value(&mut rdr)? {
        Err(io::Error::new(InvalidData, "Magic Value incorrect"))
    } else {
        Ok(rdr)
    }
}

fn read_symbol<T: Read + Seek>(rdr: &mut T) -> Result<String, io::Error> {
    rdr.seek(SeekFrom::Start(SYMBOL_OFFSET))?;
    let mut buffer = [0; SYMBOL_LEN];
    rdr.read_exact(&mut buffer)?;
    let ret = str::from_utf8(&buffer).unwrap().trim().to_owned();
    Ok(ret)
}

fn read_len<T: Read + Seek>(rdr: &mut T) -> Result<u64, io::Error> {
    rdr.seek(SeekFrom::Start(LEN_OFFSET))?;
    rdr.read_u64::<BigEndian>()
}

fn read_min_ts<T: Read + Seek>(rdr: &mut T) -> Result<u64, io::Error> {
    Ok(read_first(rdr)?.ts)
}

fn read_max_ts<T: Read + Seek>(rdr: &mut T) -> Result<u64, io::Error> {
    rdr.seek(SeekFrom::Start(MAX_TS_OFFSET))?;
    rdr.read_u64::<BigEndian>()
}

/// get updates within time range from file
pub fn get_range_in_file(fname: &str, min_ts: u64, max_ts: u64) -> Result<Vec<Update>, io::Error> {
    let mut rdr = file_reader(fname)?;
    range(&mut rdr, min_ts, max_ts)
}

/// reads a vector of Update over some time interval (min_ts, max_ts) from file.
/// :param min_ts is time in millisecond
/// :param max_ts is time in millisecond
pub fn range<T: Read + Seek>(rdr: &mut T, min_ts: u64, max_ts: u64) -> Result<Vec<Update>, io::Error> {
    let mut v: Vec<Update> = Vec::with_capacity(2048);
    range_for_each(rdr, min_ts, max_ts, &mut |up| {v.push(*up)})?;
    Ok(v)
}

fn range_for_each<T: Read + Seek, F: for<'a> FnMut(&'a Update)>(rdr: &mut T, min_ts: u64, max_ts: u64, f: &mut F) -> Result<(), io::Error> {
    // convert ts to match the dtf file format (in ms)

    // can't go back in time
    if min_ts > max_ts {
        return Ok(());
    }
    // go to beginning of main section
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

    loop {
        // read marker byte
        match rdr.read_u8() {
            Ok(byte) => {
                if byte != 0x1 {
                    return Ok(());
                }
            }   // 0x1 indicates a batch
            Err(_e) => {
                return Ok(());
            }                        // EOF
        };

        // read the metadata of the current batch
        let current_meta = read_one_batch_meta(rdr);
        let current_ref_ts = current_meta.ref_ts;

        // skip a few bytes and read the next metadata
        let bytes_to_skip = current_meta.count as usize * BYTES_PER_ROW;
        rdr.seek(SeekFrom::Current(bytes_to_skip as i64)).expect(
            &format!(
                "Skipping {} rows",
                current_meta.count
            ),
        );

        // must be a batch
        match rdr.read_u8() {
            Ok(byte) => {
                if byte != 0x1 {
                    return Ok(());
                }
            }   // is a batch
            Err(_e) => {
                return Ok(());
            }                        // EOF
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
            return Ok(());
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
        if (min_ts <= current_ref_ts && max_ts <= next_ref_ts)
                   || (min_ts < next_ref_ts && max_ts >= next_ref_ts)
                   || (min_ts > current_ref_ts && max_ts < next_ref_ts)
        {
            // seek back
            let bytes_to_scrollback = - (bytes_to_skip as i64) - 14 /* metadata */ - 1 /* indicator byte */ ;
            rdr.seek(SeekFrom::Current(bytes_to_scrollback)).expect(
                "scrolling back",
            );
            //   |1*------|1--          <- we are here
            // read and filter current batch
            if min_ts <= current_ref_ts && max_ts >= next_ref_ts {
                read_one_batch_main_for_each(rdr, current_meta, f)?;
            } else {
                read_one_batch_main_for_each(rdr, current_meta, &mut |up| {
                    if up.ts <= max_ts && up.ts >= min_ts {
                        f(up);
                    }
                })?;
            }

        //               [      ]
        // |1----|1---|1----
        //
        } else if min_ts >= next_ref_ts {
            // simply skip back to the beginning of the second batch
            // |1----*|1---|1---
            let bytes_to_scrollback = - 14 /* metadata */ - 1 /* indicator byte */ ;
            rdr.seek(SeekFrom::Current(bytes_to_scrollback)).expect(
                "SKIPPING n ROWS",
            );
        } else {
            panic!("{}, {}, {}, {}..... Should have covered all the cases.", min_ts, max_ts, current_ref_ts, next_ref_ts);
        }
    }
}

/// Read metadata block and main batch block
pub fn read_one_batch<R: Read + Seek>(rdr: &mut R) -> Result<Vec<Update>, io::Error> {
    let is_ref = rdr.read_u8()? == 0x1;
    if !is_ref {
        Ok(vec![])
    } else {
        let meta = read_one_batch_meta(rdr);
        read_one_batch_main(rdr, meta)
    }
}

/// Read metadata block and main batch block
pub fn read_one_batch_for_each<R: Read + Seek, F: for<'a> FnMut(&'a Update)>(rdr: &mut R, f: &mut F) -> Result<(), io::Error> {
    let is_ref = rdr.read_u8()? == 0x1;
    if !is_ref {
        Ok(())
    } else {
        let meta = read_one_batch_meta(rdr);
        read_one_batch_main_for_each(rdr, meta, f)
    }
}

/// reach one `BatchMetadata` block
pub fn read_one_batch_meta(rdr: &mut impl Read) -> BatchMetadata {
    let ref_ts = rdr.read_u64::<BigEndian>().unwrap();
    let ref_seq = rdr.read_u32::<BigEndian>().unwrap();
    let count = rdr.read_u16::<BigEndian>().unwrap();

    BatchMetadata {
        ref_ts,
        ref_seq,
        count,
    }
}

fn read_one_batch_main_for_each<R: Read + Seek, F: for<'a> FnMut(&'a Update)>(rdr: &mut R, meta: BatchMetadata, f: &mut F) -> Result<(), io::Error> {
    for _i in 0..meta.count {
        let up = read_one_update(rdr, &meta)?;
        f(&up);
    }
    Ok(())
}

fn read_one_batch_main(rdr: &mut (impl Read + Seek), meta: BatchMetadata) -> Result<Vec<Update>, io::Error> {
    let mut v: Vec<Update> = vec![];
    for _i in 0..meta.count {
        let up = read_one_update(rdr, &meta)?;
        v.push(up);
    }
    Ok(v)
}

fn read_one_update(rdr: &mut (impl Read + Seek), meta: &BatchMetadata) -> Result<Update, io::Error> {
    let ts = u64::from(rdr.read_u16::<BigEndian>()?) + meta.ref_ts;
    let seq = u32::from(rdr.read_u8()?) + meta.ref_seq;
    let flags = rdr.read_u8()?;
    let is_trade = ( Flags::from_bits(flags).ok_or_else(||{ InvalidData })? & Flags::FLAG_IS_TRADE).to_bool();
    let is_bid = ( Flags::from_bits(flags).ok_or_else(||{ InvalidData })? & Flags::FLAG_IS_BID).to_bool();
    let price = rdr.read_f32::<BigEndian>()?;
    let size = rdr.read_f32::<BigEndian>()?;
    Ok(Update {
        ts,
        seq,
        is_trade,
        is_bid,
        price,
        size,
    })
}

fn read_first_batch<T: Read + Seek>(mut rdr: &mut T) -> Result<Vec<Update>, io::Error> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    read_one_batch(&mut rdr)
}

fn read_first<T: Read + Seek>(mut rdr: &mut T) -> Result<Update, io::Error> {
    let batch = read_first_batch(&mut rdr)?;
    Ok(batch[0].clone())
}

/// Get number of updates in file
pub fn get_size(fname: &str) -> Result<u64, io::Error> {
    let mut rdr = file_reader(fname)?;
    read_len(&mut rdr)
}

/// Read Metadata block from buffer
pub fn read_meta_from_buf<T: Read + Seek>(mut rdr: &mut T) -> Result<Metadata, io::Error> {
    let symbol = read_symbol(&mut rdr)?;
    let count = read_len(&mut rdr)?;
    let max_ts = read_max_ts(&mut rdr)?;
    let min_ts = if count > 0 {
        read_min_ts(&mut rdr)?
    } else {
        max_ts
    };

    Ok(Metadata {
        symbol,
        count,
        max_ts,
        min_ts,
    })
}

/// Read Metadata file from file
pub fn read_meta(fname: &str) -> Result<Metadata, io::Error> {
    let mut rdr = file_reader(fname)?;
    read_meta_from_buf(&mut rdr)
}

/// iterator interface for reading DTF files
pub mod iterators {

    use super::*;

    /// read batch metadata from dtf files
    pub struct DTFMetadataReader<T: Read + Seek> {
        rdr: T
    }

    impl<T: Read + Seek> DTFMetadataReader<T> {
        /// create a new DTFBufReader
        pub fn new(mut rdr: T) -> Self {
            rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
            DTFMetadataReader {
                rdr,
            }
        }
    }

    impl<T: Read + Seek> Iterator for DTFMetadataReader<T> {
        type Item = BatchMetadata;
        fn next(&mut self) -> Option<Self::Item> {
            if let Ok(is_ref) = self.rdr.read_u8() {
                if is_ref == 0x1 {
                    let meta = read_one_batch_meta(&mut self.rdr);
                    self.rdr.seek(SeekFrom::Current(meta.count as i64 * BYTES_PER_ROW as i64)).unwrap();
                    Some(meta)
                } else { None }
            } else { None }
        }
    }

    /// BufReader for DTF files with batch block of size `block_size`
    #[derive(Clone, Debug)]
    pub struct DTFBufReader<T: Read + Seek> {
        rdr: T,
        current_meta: Option<BatchMetadata>,
        /// total number of updates
        n_up: u64,
        /// index of the last update to read
        last_idx: Option<u32>,
        i_up_in_file: u32,
        i_up: u32,
    }

    impl<T: Read + Seek> DTFBufReader<T> {

        /// start at i-th update in file
        pub fn with_offset(mut rdr: T, offset: usize) -> Self {
            let meta = read_meta_from_buf(&mut rdr).unwrap();
            rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

            let mut dtf = DTFBufReader {
                rdr,
                current_meta: None,
                n_up: meta.count,
                last_idx: None,
                i_up_in_file: 0,
                i_up: 0,
            };

            dtf.next_block().unwrap();
            let mut cur = 0;
            while cur < offset {
                let count = dtf.current_meta.as_ref().unwrap().count;
                if (offset - cur) < count as usize {
                    let skip_bytes = (offset - cur) as i64 * 12;
                    dtf.rdr.seek(SeekFrom::Current(skip_bytes)).unwrap();
                    dtf.i_up = (offset - cur) as u32;
                    cur = offset;
                    dtf.i_up_in_file = offset as u32;
                } else {
                    let skip_bytes = count as i64 * 12;
                    dtf.rdr.seek(SeekFrom::Current(skip_bytes)).unwrap();

                    cur += count as usize;
                    dtf.next_block().unwrap();
                }
            }
            dtf
        }

        /// create a new DTFBufReader
        pub fn new(mut rdr: T) -> Self {
            let meta = read_meta_from_buf(&mut rdr).unwrap();
            rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
            DTFBufReader {
                rdr,
                current_meta: None,
                n_up: meta.count,
                last_idx: None,
                i_up_in_file: 0,
                i_up: 0,
            }
        }

        /// reset iterator
        pub fn reset(&mut self) {
            self.rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
            self.current_meta = None;
            self.last_idx = None;
            self.i_up_in_file = 0;
            self.i_up = 0;
        }

        /// Get 0-indexed update position of cursor in the file
        pub fn current_update_index(&self) -> u32 {
            self.i_up
        }

        /// set last update index to read
        pub fn to_end(mut self) -> Self {
            self.last_idx = None;
            self
        }

        /// set last update index to read
        pub fn to(mut self, i: u32) -> Self {
            self.last_idx = Some(i);
            self
        }

        fn next_block(&mut self) -> Option<()> {
            if let Ok(is_ref) = self.rdr.read_u8() {
                if is_ref == 0x1 {
                    let meta = read_one_batch_meta(&mut self.rdr);
                    self.current_meta = Some(meta);
                    self.i_up = 0;
                    Some(())
                } else {
                    None
                }
            } else {
                None
            }
        }
        fn read_one(&mut self) -> Update {
            let up = read_one_update(&mut self.rdr, &self.current_meta.as_ref().unwrap()).unwrap();
            self.i_up += 1;
            self.i_up_in_file += 1;
            up
        }
    }

    impl<'a, T: Read + Seek> Iterator for &'a mut DTFBufReader<T> {
        type Item = Update;
        fn next(&mut self) -> Option<Self::Item> {
            if let Some(end) = self.last_idx {
                if self.i_up_in_file >= end {
                    return None;
                }
            }
            if self.current_meta.is_none() {
                self.next_block()?;
            }
            if (self.i_up as u64) >= self.n_up {
                return None;
            }
            if (self.i_up as u16) >= self.current_meta.as_ref().unwrap().count
            {
                self.next_block()?;
            }
            Some(self.read_one())
        }
    }
}


fn read_n_batches_for_each<T: Read + Seek, F: for<'a> FnMut(&'a Update)>(mut rdr: &mut T, num_rows: u32, f: &mut F) -> Result<(), io::Error> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    let mut count = 0;
    if num_rows == 0 { return Ok(()); }
    while let Ok(is_ref) = rdr.read_u8() {
        if is_ref == 0x1 {
            rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
            read_one_batch_for_each(&mut rdr, f)?;
        }
        count += 1;
        if count > num_rows {
            break;
        }
    }
    Ok(())
}

fn read_n_batches<T: Read + Seek>(mut rdr: &mut T, num_rows: u32) -> Result<Vec<Update>, io::Error> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    let mut v: Vec<Update> = Vec::with_capacity(num_rows as usize);
    let mut count = 0;
    if num_rows == 0 { return Ok(v); }
    while let Ok(is_ref) = rdr.read_u8() {
        if is_ref == 0x1 {
            rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
            v.extend(read_one_batch(&mut rdr)?);
        }
        count += 1;
        if count > num_rows {
            break;
        }
    }
    Ok(v)
}

fn read_all_for_each<T: Read + Seek, F: for<'a> FnMut(&'a Update)>(mut rdr: &mut T, f: &mut F) -> Result<(), io::Error> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    while let Ok(is_ref) = rdr.read_u8() {
        if is_ref == 0x1 {
            rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
            read_one_batch_for_each(&mut rdr, f)?;
        }
    }
    Ok(())
}

fn read_all<T: Read + Seek>(mut rdr: &mut T) -> Result<Vec<Update>, io::Error> {
    let len = read_len(&mut rdr)?;
    let mut v: Vec<Update> = Vec::with_capacity(len as usize);
    read_all_for_each(rdr, &mut |up| v.push(*up))?;
    Ok(v)
}

/// Decode the main section in a dtf file.
/// Optionally read all or some `num_rows` batches.
pub fn decode(fname: &str, num_rows: Option<u32>) -> Result<Vec<Update>, io::Error> {
    let mut rdr = file_reader(fname)?;
    match num_rows {
        Some(num_rows) => read_n_batches(&mut rdr, num_rows),
        None => read_all(&mut rdr),
    }
}

/// Decode the main section in a dtf file.
/// Optionally read all or some `num_rows` batches.
/// Pass in a closure for each update
pub fn decode_for_each<F: for<'a> FnMut(&'a Update)>(fname: &str, num_rows: Option<u32>, f: &mut F) -> Result<(), io::Error> {
    let mut rdr = file_reader(fname)?;
    match num_rows {
        Some(num_rows) => read_n_batches_for_each(&mut rdr, num_rows, f),
        None => read_all_for_each(&mut rdr, f),
    }
}

/// Decode an entire buffer to Updates
pub fn decode_buffer(mut buf: &mut (impl Read + Seek)) -> Vec<Update> {
    let mut v = vec![];
    let mut res = read_one_batch(&mut buf);
    while let Ok(ups) = res {
        v.extend(ups);
        res = read_one_batch(&mut buf);
    }
    v
}

/// append a list of Updates to file
/// Panic when range is wrong (new_min_ts <= old_max_ts)
#[cfg_attr(feature = "count_alloc", count_alloc)]
pub fn append(fname: &str, ups: &[Update]) -> Result<(), io::Error> {
    let mut rdr = file_reader(fname)?;
    let _symbol = read_symbol(&mut rdr)?;

    let old_max_ts = read_max_ts(&mut rdr)?;

    let mut ups = ups.into_iter().filter(|up| up.ts > old_max_ts).peekable();

    if ups.peek().is_none() {
        return Ok(());
    }

    let new_min_ts = ups.clone().next().unwrap().ts;
    let new_max_ts = ups.clone().next_back().unwrap().ts;

    if new_min_ts <= old_max_ts {
        panic!("Cannot append data!(not implemented)");
    }

    let cur_len = read_len(&mut rdr)?;

    let new_len = cur_len + ups.clone().count() as u64;

    let mut wtr = file_writer(fname, false)?;
    write_len(&mut wtr, new_len)?;
    write_max_ts(&mut wtr, new_max_ts)?;

    if cur_len == 0 {
        wtr.seek(SeekFrom::Start(MAIN_OFFSET)).unwrap();
    } else {
        wtr.seek(SeekFrom::End(0)).unwrap();
    }
    write_batches(&mut wtr, ups)?;
    wtr.flush().unwrap();

    Ok(())
}

/// search every matching dtf file under folder for timestamp range
pub fn scan_files_for_range(
    folder: &str,
    symbol: &str,
    min_ts: u64,
    max_ts: u64,
) -> Result<Vec<Update>, io::Error> {
    let mut ret = Vec::with_capacity(1024);
    scan_files_for_range_for_each(folder, symbol, min_ts, max_ts, &mut|up|{ret.push(*up)})?;
    Ok(ret)
}

/// search every matching dtf file under folder for timestamp range
pub fn scan_files_for_range_for_each<F: for<'a> FnMut(&'a Update)>(
    folder: &str,
    symbol: &str,
    min_ts: u64,
    max_ts: u64,
    f: &mut F,
) -> Result<(), io::Error> {
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ))
        }
        Ok(entries) => {
            let mut v = entries
                .filter_map(|entry| {
                    let entry = entry.unwrap();
                    let fname = entry.file_name();
                    let fname = fname.to_str().unwrap().to_owned();
                    let fname = &format!("{}/{}", folder, fname);
                    let meta = read_meta(fname).ok()?;
                    Some((fname.to_owned(), meta))
                })
                .filter(|(_fname, meta)| {
                    &meta.symbol == symbol && crate::utils::within_range(min_ts, max_ts, meta.min_ts, meta.max_ts)
                })
                .collect::<Vec<_>>();

            // sort by min_ts
            v.sort_by(|(_f0, m0), (_f1, m1)| m0.cmp(m1));

            for (fname, _meta) in &v {
                eprintln!("Reading: {}", fname);
                let mut rdr = file_reader(fname)?;
                range_for_each(&mut rdr, min_ts, max_ts, f)?;
            }
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    fn real_sample_data() -> Vec<Update> {
        let fname = "../../test/test-data/bt_btcnav.dtf";
        let decoded_updates = decode(fname, None).unwrap();
        decoded_updates
    }

    #[cfg(test)]
    fn sample_data() -> Vec<Update> {
        let mut ts: Vec<Update> = vec![];
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
        let mut ts: Vec<Update> = vec![];
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
        let mut ts: Vec<Update> = vec![];
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
    fn init() -> Vec<Update> {
        let ts = sample_data();

        let fname = "test.dtf";
        let symbol = "NEO_BTC";

        encode(fname, symbol, &ts).unwrap();

        ts
    }

    #[test]
    fn should_format_metadata_properly() {
        let meta = Metadata {
            symbol: "TEST".to_owned(),
            count: 1,
            max_ts: 1,
            min_ts: 1,
        };

        assert_eq!(
            format!("{}", meta),
            r#"{
  "symbol": "TEST",
  "count": 1,
  "max_ts": 1,
  "max_ts_human": "1970-01-01 00:00:00 UTC",
  "min_ts": 1,
  "min_ts_human": "1970-01-01 00:00:00 UTC"
}"#
        );
    }

    #[test]
    fn should_encode_decode_one_item() {
        let ts = sample_data_one_item();
        let fname = "test.dtf";
        let symbol = "NEO_BTC";
        encode(fname, symbol, &ts).unwrap();
        let decoded_updates = decode(fname, None).unwrap();
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    fn should_encode_decode_real() {
        let ts = real_sample_data();
        let fname = "realtest.dtf";
        let symbol = "bnc_zrx_btc";
        encode(fname, symbol, &ts).unwrap();
        let decoded_updates = decode(fname, None).unwrap();
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    fn should_encode_and_decode_file() {
        let ts = init();
        let fname = "test.dtf";
        let decoded_updates = decode(fname, None).unwrap();
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    fn should_return_the_correct_range() {
        let fname = "test.dtf";
        {
            // let wtr = file_writer(fname, true);
            let ups = (1..50)
                .map(|i| {
                    Update {
                        ts: i * 1000 as u64,
                        seq: i as u32,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false,
                    }
                })
                .collect::<Vec<Update>>();

            encode(fname, "test", &ups).unwrap();
        }

        let mut rdr = file_reader(fname).unwrap();
        let should_be = (10..21)
            .map(|i| {
                Update {
                    ts: i * 1000 as u64,
                    seq: i as u32,
                    price: 0.,
                    size: 0.,
                    is_bid: false,
                    is_trade: false,
                }
            })
            .collect::<Vec<Update>>();
        assert_eq!(should_be, range(&mut rdr, 10000, 20000).unwrap());
    }

    #[test]
    fn should_return_the_correct_range_2() {
        let fname = "test.dtf";
        {
            // let wtr = file_writer(fname, true);
            let ups = (1..1000)
                .map(|i| {
                    Update {
                        ts: i * 1000 as u64,
                        seq: i as u32 % 500 * 500,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false,
                    }
                })
                .collect::<Vec<Update>>();

            encode(fname, "test", &ups).unwrap();
        }

        let mut rdr = file_reader(fname).unwrap();
        assert_eq!(
            (1..999)
                .map(|i| {
                    Update {
                        ts: i * 1000 as u64,
                        seq: i as u32 % 500 * 500,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false,
                    }
                })
                .collect::<Vec<Update>>(),
            range(&mut rdr, 1000, 999000).unwrap()
        ); // ???
    }

    #[test]
    fn should_return_correct_range_real() {
        let fname: &str = "../../test/test-data/bt_btcnav.dtf";
        let mut rdr = file_reader(fname).unwrap();

        let start = 1_510_168_156 * 1000;
        let end = 1_510_171_756 * 1000;

        let ups = range(&mut rdr, start, end).unwrap();
        println!("{}", ups.len());
        assert_eq!(ups.len(), 10736);

        for up in ups.iter() {
            assert!(up.ts >= start && up.ts <= end);
        }
    }

    // TODO: write more test cases...

    #[test]
    fn should_return_correct_symbol() {
        init();
        let fname = "test.dtf";
        let mut rdr = file_reader(fname).unwrap();
        let sym = read_symbol(&mut rdr).unwrap();
        assert_eq!(sym, "NEO_BTC");
    }

    #[test]
    fn should_return_first_record() {
        let vs = init();
        let fname = "test.dtf";
        let mut rdr = file_reader(fname).unwrap();
        let v = read_first(&mut rdr).unwrap();
        assert_eq!(vs[0], v);
    }

    #[test]
    fn should_return_correct_num_of_items() {
        let vs = init();
        let fname = "test.dtf";
        let mut rdr = file_reader(fname).unwrap();
        let len = read_len(&mut rdr).unwrap();
        assert_eq!(vs.len() as u64, len);
    }

    #[test]
    fn should_return_max_ts() {
        let vs = init();
        let fname = "test.dtf";
        let mut rdr = file_reader(fname).unwrap();
        let max_ts = read_max_ts(&mut rdr).unwrap();
        assert_eq!(max_ts, get_max_ts_sorted(&vs));
    }

    #[test]
    fn should_append_filtered_data() {
        should_encode_and_decode_file();

        println!("----DONE----");

        let fname = "test.dtf";
        let old_data = sample_data();
        let old_max_ts = get_max_ts_sorted(&old_data);
        let append_data: Vec<Update> = sample_data_append()
            .into_iter()
            .filter(|up| up.ts >= old_max_ts)
            .collect();
        let new_size = append_data.len() + old_data.len();

        append(fname, &append_data).unwrap();

        println!("----APPENDED----");

        let mut rdr = file_reader(fname).unwrap();

        // max_ts
        let max_ts = read_max_ts(&mut rdr).unwrap();
        assert_eq!(max_ts, get_max_ts_sorted(&append_data));

        // total len
        let mut rdr = file_reader(fname).unwrap();
        let len = read_len(&mut rdr).unwrap();
        assert_eq!(new_size as u64, len);

        let mut all_the_data = sample_data();
        all_the_data.extend(append_data);
        all_the_data.sort();
        let decoded = decode(&fname, None).unwrap();
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
        assert_eq!(r#"{"ts":20000.001,"seq":113,"is_trade":false,"is_bid":false,"price":5100.01,"size":1.1456456}"#, t1.as_json());
    }

    #[test]
    fn should_write_to_bytes() {
        // ADD 0,0,f,f,0,0;
        let up = Update {
            ts: 0,
            seq: 0,
            is_trade: false,
            is_bid: false,
            price: 0.,
            size: 0.,
        };
        let mut bytes = vec![];
        write_batches(&mut bytes, [up].into_iter().peekable()).unwrap();
        assert_eq!(
            vec![
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            bytes
        );
    }

    #[test]
    fn test_write_batches() {
        let mut ups = vec![];
        for _ in 0..1000000 {
            let up = Update {
                ts: 100,
                seq: 10,
                is_trade: true,
                is_bid: true,
                price: 9999.999,
                size: 9999.999,
            };
            ups.push(up);
        }
        let mut bytes = vec![];
        write_batches(&mut bytes, ups.iter().peekable()).unwrap();
        dbg!(bytes.len());
        let mut cur = Cursor::new(bytes);

        let mut v: Vec<Update> = Vec::new();
        while let Ok(is_ref) = cur.read_u8() {
            if is_ref == 0x1 {
                cur.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
                v.extend(read_one_batch(&mut cur).unwrap());
            }
        }

        assert_eq!(v.len(), 1000000);
    }

    #[test]
    fn test_iterator() {

        let fname = "../../test/test-data/bt_btcnav.dtf";

        let mut it1 = &mut iterators::DTFBufReader::new(file_reader(fname).unwrap());
        it1.next();
        let res1 = it1.next().unwrap();

        let mut it2 = &mut iterators::DTFBufReader::with_offset(file_reader(fname).unwrap(), 1);
        let res2 = it2.next().unwrap();
        assert_eq!(res1, res2);
        assert_eq!(it1.next().unwrap(), it2.next().unwrap());
        assert_eq!(it1.next().unwrap(), it2.next().unwrap());
        assert_eq!(it1.next().unwrap(), it2.next().unwrap());

    }

    #[test]
    fn test_iterator_large() {
        let fname = "../../test/test-data/bt_btcnav.dtf";
        let mut it1 = &mut iterators::DTFBufReader::new(file_reader(fname).unwrap());
        (0..10000).for_each(|_| { it1.next().unwrap(); });
        let res1 = it1.next().unwrap();

        let mut it2 = &mut iterators::DTFBufReader::with_offset(file_reader(fname).unwrap(), 10000);
        let res2 = it2.next().unwrap();
        assert_eq!(res1, res2);
        assert_eq!(it1.next().unwrap(), it2.next().unwrap());
    }

    #[test]
    fn test_iterator_every() {
        let fname = "../../test/test-data/bt_btcnav.dtf";

        let count = read_meta(fname).unwrap().count;
        dbg!(count);

        for i in (0..(count as usize)).step_by(10000) {
            dbg!(i);
            let mut it = &mut iterators::DTFBufReader::with_offset(file_reader(fname).unwrap(), i);
            while let Some(_) = it.next() {};
        }
    }

    #[test]
    fn test_iterator_with_last_idx() {
        let fname = "../../test/test-data/bt_btcnav.dtf";
        let mut it1 = &mut iterators::DTFBufReader::new(file_reader(fname).unwrap()).to(10001);
        (0..10000).for_each(|_| { it1.next().unwrap(); });
        it1.next().unwrap();
        assert!(it1.next().is_none());
    }
}
