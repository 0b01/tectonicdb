use libtectonic::utils;
use libtectonic::dtf::update::Update;
use byteorder::{ReadBytesExt, BigEndian};
use std::io::{Read, Cursor};

pub fn parse_raw_line(buf: &[u8]) -> Option<(Option<Update>, Option<String>)> {
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

/// Parses a line that looks like
///
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
///
/// into an `Update` struct.
///
pub fn parse_line(string: &str) -> Option<Update> {
    let mut u = Update {
        ts: 0,
        seq: 0,
        is_bid: false,
        is_trade: false,
        price: -0.1,
        size: -0.1,
    };
    let mut buf: String = String::new();
    let mut count = 0;
    let mut most_current_bool = false;

    for ch in string.chars() {
        if ch == '.' && count == 0 {
            continue;
        } else if (ch == '.' && count != 0) || ch.is_digit(10) {
            buf.push(ch);
        } else if ch == 't' || ch == 'f' {
            most_current_bool = ch == 't';
        } else if ch == ',' || ch == ';' {
            match count {
                0 => {
                    u.ts = match buf.parse::<u64>() {
                        Ok(ts) => utils::fill_digits(ts),
                        Err(_) => return None,
                    }
                }
                1 => {
                    u.seq = match buf.parse::<u32>() {
                        Ok(seq) => seq,
                        Err(_) => return None,
                    }
                }
                2 => {
                    u.is_trade = most_current_bool;
                }
                3 => {
                    u.is_bid = most_current_bool;
                }
                4 => {
                    u.price = match buf.parse::<f32>() {
                        Ok(price) => price,
                        Err(_) => return None,
                    }
                }
                5 => {
                    u.size = match buf.parse::<f32>() {
                        Ok(size) => size,
                        Err(_) => return None,
                    }
                }
                _ => panic!("IMPOSSIBLE"),
            }
            count += 1;
            buf.clear();
        }
    }

    if u.price < 0. || u.size < 0. {
        None
    } else {
        Some(u)
    }
}

pub fn parse_dbname(string: &str) -> (usize, &str) {
    let into_indices: Vec<_> = string.match_indices(" INTO ").collect();
    let (index, _) = into_indices[0];
    let dbname = &string[(index + 6)..];
    (index, dbname)
}

/// returns Option<Update, dbname>
pub fn parse_add_into(string: &str) -> (Option<Update>, Option<String>) {
    let (index, dbname) = parse_dbname(string);
    let data_string: &str = {
        if string.contains("ADD ") {
            &string[4..(index)]
        } else if string.contains("INSERT ") {
            &string[7..(index)]
        } else {
            return (None, None);
        }
    };

    match parse_line(data_string) {
        Some(up) => (Some(up), Some(dbname.to_owned())),
        None => (None, None),
    }
}

pub fn parse_get_range(string: &str) -> Option<(u64, u64)> {
    if string.contains(" FROM ") {
        // range to query
        let from_epoch = &string[(string.find(" FROM ").unwrap() + 6)..]
            .split(' ')
            .collect::<Vec<&str>>()
            [0]
            .parse::<u64>()
            .unwrap() * 1000;
        let to_epoch = &string[(string.find(" TO ").unwrap() + 4)..]
            .split(' ')
            .collect::<Vec<&str>>()
            [0]
            .parse::<u64>()
            .unwrap() * 1000;
        Some((from_epoch, to_epoch))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_parse_string_not_okay() {
        let string = "1505177459.658, 139010,,, f, t, 0.0703629, 7.65064249;";
        assert!(parse_line(&string).is_none());
        let string = "150517;";
        assert!(parse_line(&string).is_none());
        let string = "something;";
        assert!(parse_line(&string).is_none());
    }

    #[test]
    fn should_parse_string_okay() {
        let string = "1505177459.658, 139010, f, t, 0.0703629, 7.65064249;";
        let target = Update {
            ts: 1505177459658,
            seq: 139010,
            is_trade: false,
            is_bid: true,
            price: 0.0703629,
            size: 7.65064249,
        };
        assert_eq!(target, parse_line(&string).unwrap());


        let string1 = "1505177459.65, 139010, t, f, 0.0703620, 7.65064240;";
        let target1 = Update {
            ts: 1505177459650,
            seq: 139010,
            is_trade: true,
            is_bid: false,
            price: 0.0703620,
            size: 7.65064240,
        };
        assert_eq!(target1, parse_line(&string1).unwrap());
    }

    #[test]
    fn should_parse_dbname_ok() {
        assert_eq!(parse_dbname("INSERT 1 INTO dbname"), (8, "dbname"));
        assert_eq!(parse_dbname("INSERT 1000, INTO dbname1;"), (12, "dbname1;"));
    }

    #[test]
    fn should_parse_add_into_ok() {
        let cmd = "INSERT 1505177459.65, 139010, t, f, 0.0703620, 7.65064240; INTO dbname";
        println!("{:?}", parse_add_into(cmd));
        let target = Update {
            ts: 1505177459650,
            seq: 139010,
            is_trade: true,
            is_bid: false,
            price: 0.0703620,
            size: 7.65064240,
        };
        assert_eq!(
            (Some(target), Some("dbname".to_owned())),
            parse_add_into(cmd)
        );
    }

    #[test]
    fn should_parse_default_ok() {
        let cmd = "ADD 0,0,f,f,0,0; INTO default";
        println!("{:?}", parse_add_into(cmd));
        let target = Update {
            ts: 0,
            seq: 0,
            is_trade: false,
            is_bid: false,
            price: 0.,
            size: 0.,
        };
        assert_eq!(
            (Some(target), Some("default".to_owned())),
            parse_add_into(cmd)
        );
    }

}
