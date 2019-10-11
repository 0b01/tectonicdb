extern crate chrono;
use crate::dtf::update::Update;
use self::chrono::{ NaiveDateTime, DateTime, Utc };
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Error, Read, Write, Cursor};

/// fill digits 123 => 12300 etc..
/// 151044287500 => 1510442875000
pub fn fill_digits(input: u64) -> u64 {
    let mut ret = input;
    if input == 0 {
        0
    } else {
        while ret < 1_000_000_000_000 {
            // println!("{}", ret);
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
    target_min <= file_max && target_max >= file_min
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
pub fn encode_insert_into(book_name: Option<String>, update: &Update) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    let len = match &book_name {
        None => 0u64,
        Some(book_name) => book_name.len() as u64
    };
    buf.write(&len.to_be_bytes())?;
    if let Some(book_name) = book_name {
        buf.write(book_name.as_bytes())?;
    }
    buf.write(&update.serialize_raw())?;
    Ok(buf)
}

///  the inverse of encode_insert_into
pub fn decode_insert_into(buf: &[u8]) -> Option<(Option<Update>, Option<String>)> {
    let mut rdr = Cursor::new(buf);

    let len = rdr.read_u64::<BigEndian>().ok()?;
    let mut book_name_buf = Vec::new();

    let book_name = if len > 0 {
        rdr.by_ref().take(len).read(&mut book_name_buf).ok()?;
        Some(std::str::from_utf8(&book_name_buf).unwrap().to_owned())
    } else {
        None
    };
    let update = Update::from_raw(&rdr.into_inner()).ok();

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
}
