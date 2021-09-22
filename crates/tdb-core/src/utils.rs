extern crate chrono;
use crate::dtf::update::Update;
use self::chrono::{ NaiveDateTime, DateTime, Utc };
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Error, Seek, SeekFrom, Write, Cursor};
use std::io::BufWriter;
type BookName = arrayvec::ArrayString<64>;

/// fill digits 123 => 12300 etc..
/// 151044287500 => 1510442875000
pub fn fill_digits(input: u64) -> u64 {
    let mut ret = input;
    if input == 0 {
        0
    } else {
        while ret < 1_000_000_000_000 {
            // info!("{}", ret);
            ret *= 10;
        }
        ret
    }
}

/// Returns bigram
///     bigram(&[1,2,3]) -> [(1,2), (2,3)]
pub fn bigram<T: Copy>(a: &[T]) -> Vec<(T, T)> {
    a.into_iter()
        .map(|&t| t)
        .zip(a[1..].into_iter().map(|&t| t))
        .collect::<Vec<(_, _)>>()
}

/// check if two ranges intersect
pub fn within_range(target_min: u64, target_max: u64, file_min: u64, file_max: u64) -> bool {
    target_min <= file_max || target_max >= file_min
}

/// converts epoch time to human readable string
pub fn epoch_to_human(ts: u64) -> String {
    let naive_datetime = NaiveDateTime::from_timestamp(ts as i64, 0);
    let datetime_again: DateTime<Utc> = DateTime::from_utc(naive_datetime, Utc);

    format!("{}", datetime_again)

}

/// client inserts an update into server
/// binary form of
///     INSERT [update] INTO [book]
pub fn encode_insert_into(book_name: Option<&str>, update: &Update) -> Result<Vec<u8>, Error> {
    let mut buf = BufWriter::new(Vec::with_capacity(64*30));
    buf.write(crate::RAW_INSERT_PREFIX)?;
    let len = match &book_name {
        None => 0u64,
        Some(book_name) => book_name.len() as u64
    };
    buf.write(&len.to_be_bytes())?;
    if let Some(book_name) = book_name {
        buf.write(book_name.as_bytes())?;
    }
    update.serialize_raw_to_buffer(&mut buf)?;
    buf.write(&[b'\n'])?;
    Ok(buf.into_inner().unwrap())
}

///  the inverse of encode_insert_into
pub fn decode_insert_into<'a>(buf: &'a [u8]) -> Option<(Option<Update>, Option<BookName>)> {
    let mut rdr = Cursor::new(buf);
    rdr.seek(SeekFrom::Current(crate::RAW_INSERT_PREFIX.len() as i64)).ok()?;
    let len = rdr.read_u64::<BigEndian>().ok()? as usize;
    let book_name = if len > 0 {
        // let mut book_name_buf = vec![0; len as usize];
        // rdr.read_exact(&mut book_name_buf).ok()?;
        let pos = rdr.position() as usize;
        let name = unsafe { std::str::from_utf8_unchecked(&rdr.get_ref()[pos..(pos+len)]) };
        let name = BookName::from(name).ok()?;
        rdr.set_position((pos + len) as u64);
        Some(name)
    } else {
        None
    };

    let pos = rdr.position() as usize;
    let buf = rdr.into_inner();
    let update = Update::from_raw(&buf[pos..]).ok();
    Some((update, book_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bigram() {
        let a = vec![1, 2, 3];
        assert_eq!(bigram(&a), vec![(1, 2), (2, 3)]);
    }

    #[test]
    fn test_stringify_epoch() {
        let epoch = 1518488928;
        assert_eq!("2018-02-13 02:28:48 UTC", epoch_to_human(epoch));
    }

    #[test]
    fn test_encode_decode_insert_into() {
        let book_name = Some("bnc_btc_eth");
        let update = Update { ts: 1513922718770, seq: 0, is_bid: true, is_trade: false, price: 0.001939,  size: 22.85 };
        let encoded = encode_insert_into(book_name, &update).unwrap();
        let (decoded_update, decoded_book_name) = decode_insert_into(&encoded).unwrap();
        assert_eq!(decoded_book_name.unwrap().as_str(), book_name.unwrap());
        assert_eq!(&decoded_update, &Some(update));
    }
}
