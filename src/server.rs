/// Server should handle requests similar to Redis
/// 
/// PING
/// 
/// INFO
/// 
/// DB neo_btc
/// 
/// ADD ts, seq, is_trade, bool, price, size
/// 
/// GET neo_btc 2017-06-10 TO 2018-09-20 LIMIT 100
/// 
/// BULKADD
/// ts, seq, is_trade, is_bid, price, size;
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// DDAKLUB
/// 


use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;
use std::str;

use dtf;

struct State {
    db: String,
    is_adding: bool,
    v: Vec<dtf::Update>
}

fn parse_line(string : &str) -> Option<dtf::Update> {

    let mut u = dtf::Update { ts : 0, seq : 0, is_bid : false, is_trade : false, price : -0.1, size : -0.1 };
    let mut buf : String = String::new();
    let mut count = 0;
    let mut most_current_bool = false;

    for ch in string.chars() {
        println!("{}", ch);
        if ch == '.' && count == 0 {
            continue;
        } else if ch == '.' && count != 0 {
            buf.push(ch);
        } else if ch.is_digit(10) {
            buf.push(ch);
        } else if ch == 't' || ch == 'f' {
            most_current_bool = ch == 't';
        } else if ch == ',' || ch == ';' {
            println!("{}", buf);
            match count {
                0 => { u.ts       = match buf.parse::<u64>() {Ok(ts) => ts, Err(err) => return None}},
                1 => { u.seq      = match buf.parse::<u32>() {Ok(seq) => seq, Err(err) => return None}},
                2 => { u.is_trade = most_current_bool; },
                3 => { u.is_bid   = most_current_bool; },
                4 => { u.price    = match buf.parse::<f32>() {Ok(price) => price, Err(err) => return None} },
                5 => { u.size     = match buf.parse::<f32>() {Ok(size) => size, Err(err) => return None}},
                _ => panic!("IMPOSSIBLE")
            }
            count += 1;
            buf.clear();
        }
    }

    Some(u)
}

#[test]
fn should_parse_string_not_okay() {
    let string = "1505177459.658, 139010,,, f, t, 0.0703629, 7.65064249;";
    let target = dtf::Update {
        ts: 1505177459658,
        seq: 139010,
        is_trade: false,
        is_bid: true,
        price: 0.0703629,
        size: 7.65064249
    };
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


fn gen_response(string : &str, state: &mut State) -> Option<String> {

    match string {
        "" => Some("".to_owned()),
        "PING" => Some("PONG.\n".to_owned()),
        "INFO" => Some(format!("DB: {}", state.db)),
        _ => {
            if state.is_adding {
                let parsed = parse_line(string);
                match parsed {
                    Some(up) => state.v.push(up),
                    None => return None
                }
                Some("".to_owned())
            } else

            if string.starts_with("DB") {
                let dbname : &str = &string[3..];
                state.db = dbname.to_owned();
                Some(format!("SWITCHED TO DB `{}`.\n", &dbname))
            } else

            if string.starts_with("ADD") {
                Some("".to_owned())
            } else 

            if string.starts_with("GET") {
                Some("".to_owned())
            } else

            if string.starts_with("BULKADD") {
                state.is_adding = true;
                Some("".to_owned())
            } else

            if string.starts_with("DDAKLUB") {
                state.is_adding = false;
                Some("".to_owned())
            }

            else {
                Some(format!("-ERR unknown command '{}'.\n", &string))
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 2048];

    let mut state = State {
        db: "".to_owned(),
        is_adding: false,
        v: Vec::new()
    };

    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        if bytes_read == 0 { break }
        let ping = str::from_utf8(&buf[..(bytes_read-1)]).unwrap();

        let resp = gen_response(&ping, &mut state);
        match resp {
            Some(str_resp) => stream.write(str_resp.as_bytes()).unwrap(),
            None => stream.write("ERROR".as_bytes()).unwrap()
        };
    }
}

pub fn run_server() {
    let addr = "127.0.0.1:9001";
    let listener = TcpListener::bind(addr).unwrap();
    println!("Listening on addr: {}", addr);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        thread::spawn(move || {
            handle_client(stream);
        });
    }
}