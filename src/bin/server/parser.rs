use dtf;

/// Parses a line that looks like 
/// 
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// 
/// into an `Update` struct.
/// 
pub fn parse_line(string : &str) -> Option<dtf::Update> {
    let mut u = dtf::Update { ts : 0, seq : 0, is_bid : false, is_trade : false, price : -0.1, size : -0.1 };
    let mut buf : String = String::new();
    let mut count = 0;
    let mut most_current_bool = false;

    for ch in string.chars() {
        if ch == '.' && count == 0 {
            continue;
        } else if ch == '.' && count != 0 {
            buf.push(ch);
        } else if ch.is_digit(10) {
            buf.push(ch);
        } else if ch == 't' || ch == 'f' {
            most_current_bool = ch == 't';
        } else if ch == ',' || ch == ';' {
            match count {
                0 => { u.ts       = match buf.parse::<u64>() {Ok(ts) => ts, Err(_) => return None}},
                1 => { u.seq      = match buf.parse::<u32>() {Ok(seq) => seq, Err(_) => return None}},
                2 => { u.is_trade = most_current_bool; },
                3 => { u.is_bid   = most_current_bool; },
                4 => { u.price    = match buf.parse::<f32>() {Ok(price) => price, Err(_) => return None} },
                5 => { u.size     = match buf.parse::<f32>() {Ok(size) => size, Err(_) => return None}},
                _ => panic!("IMPOSSIBLE")
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
    let target = dtf::Update {
        ts: 1505177459658,
        seq: 139010,
        is_trade: false,
        is_bid: true,
        price: 0.0703629,
        size: 7.65064249
    };
    assert_eq!(target, parse_line(&string).unwrap());


    let string1 = "1505177459.650, 139010, t, f, 0.0703620, 7.65064240;";
    let target1 = dtf::Update {
        ts: 1505177459650,
        seq: 139010,
        is_trade: true,
        is_bid: false,
        price: 0.0703620,
        size: 7.65064240
    };
    assert_eq!(target1, parse_line(&string1).unwrap());
}