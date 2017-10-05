/// Server should handle requests similar to Redis
/// 
/// PING
/// 
/// INFO
/// 
/// USE neo_btc
/// 
/// CREATE neo_btc
/// 
/// ADD 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// 
/// GET ALL
/// GET 1
/// 
/// BULKADD
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// 1505177459.658, 139010, t, t, 0.0703629, 7.65064249;
/// DDAKLUB
/// 
/// FLUSH
/// FLUSHALL
/// 
/// CLEAR
/// 
/// -------------------------------------------
/// PING, INFO, USE [db], CREATE [db],
/// ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
/// BULKADD ...; DDAKLUB
/// FLUSH, FLUSHALL, GET ALL, GET [count], CLEAR


use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::Path;
use std::thread;
use std::str;

use dtf;

struct Store {
    name: String,
    v: Vec<dtf::Update>
}

impl Store {
    fn add(&mut self, new_vec : dtf::Update) {
        self.v.push(new_vec);
    }

    fn to_string(&self, count:i32) -> String {
        let objects : Vec<String> = match count {
            -1 => self.v.clone().into_iter().map(|up| up.to_json()).collect(),
            n => self.v.clone().into_iter().take(n as usize).map(|up| up.to_json()).collect()
        };

        format!("[{}]\n", objects.join(","))
    }

    fn flush(&self) -> Option<bool> {
        let fname = format!("{}.dtf", self.name);
        dtf::encode(&fname, &self.name, &self.v);
        Some(true)
    }

    fn load(&mut self) -> Option<bool> {
        let fname = format!("{}.dtf", self.name);
        if Path::new(&fname).exists() {
            self.v = dtf::decode(&fname);
            return Some(true);
        }
        None
    }

    fn clear(&mut self) {
        self.v.clear();
    }
}

struct State {
    is_adding: bool,
    store: HashMap<String, Store>,
    current_store_name: String
}

fn parse_line(string : &str) -> Option<dtf::Update> {

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
        "HELP" => Some("PING, INFO, USE [db], CREATE [db],\nADD [ts],[seq],[is_trade],[is_bid],[price],[size];\nBULKADD ... DDAKLUB, HELP\nFLUSHALL, GET ALL, GET [count]\n".to_owned()),
        "INFO" => {
            let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
            let mut json = format!(r#"{{"name": "{}", "count": {}}}"#, state.current_store_name, current_store.v.len());
            json.push('\n');
            Some(json)
        },
        "BULKADD" => {
            state.is_adding = true;
            Some("".to_owned())
        },
        "DDAKLUB" => {
            state.is_adding = false;
            Some("OK.\n".to_owned())
        },
        "GET ALL" => {
            Some(state.store.get_mut(&state.current_store_name).unwrap().to_string(-1))
        },
        "CLEAR" => {
            let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
            current_store.clear();
            Some("OK.\n".to_owned())
        },
        "FLUSH" => {
            let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
            current_store.flush();
            Some("OK\n".to_owned())
        },
        "FLUSHALL" => {
            for store in state.store.values() {
                store.flush();
            }
            Some("OK.\n".to_owned())
        },
        _ => {
            // bulkadd and add
            if state.is_adding {
                let parsed = parse_line(string);
                match parsed {
                    Some(up) => {
                        let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
                        current_store.add(up);
                    }
                    None => return None
                }
                Some("".to_owned())
            } else

            if string.starts_with("ADD ") {
                let data_string : &str = &string[3..];
                match parse_line(&data_string) {
                    Some(up) => {
                        let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
                        current_store.v.push(up);
                    }
                    None => return None
                }
                Some("OK.\n".to_owned())
            } else 

            // db commands
            if string.starts_with("CREATE ") {
                let dbname : &str = &string[7..];
                state.store.insert(dbname.to_owned(), Store {name: dbname.to_owned(), v: Vec::new()});
                Some(format!("CREATED DB `{}`.\n", &dbname))
            } else

            if string.starts_with("USE ") {
                let dbname : &str = &string[4..];
                if state.store.contains_key(dbname) {
                    state.current_store_name = dbname.to_owned();
                    let current_store = state.store.get_mut(&state.current_store_name).unwrap();
                    current_store.load();
                    Some(format!("SWITCHED TO DB `{}`.\n", &dbname))
                } else {
                    Some(format!("ERR unknown DB `{}`.\n", &dbname))
                }
            } else

            // get
            if string.starts_with("GET ") {
                let num : &str = &string[4..];
                let count = num.parse::<i32>().unwrap();
                let current_store = state.store.get_mut(&state.current_store_name).unwrap();
                Some(current_store.to_string(count))
            }

            else {
                Some(format!("ERR unknown command '{}'.\n", &string))
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 2048];

    let mut state = State {
        current_store_name: "default".to_owned(),
        is_adding: false,
        store: HashMap::new()
    };
    state.store.insert("default".to_owned(), Store {name: "default".to_owned(), v: Vec::new()});

    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        if bytes_read == 0 { break }
        let ping = str::from_utf8(&buf[..(bytes_read-1)]).unwrap();

        let resp = gen_response(&ping, &mut state);
        match resp {
            Some(str_resp) => {
                stream.write(str_resp.as_bytes()).unwrap()
                // stream.write(b">>> ").unwrap()
            }
            None => stream.write("ERR.".as_bytes()).unwrap()
        };
    }
}

pub fn run_server() {
    let addr = "127.0.0.1:9001";
    let listener = TcpListener::bind(addr).unwrap();
    println!("Listening on addr: {}", addr);

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        thread::spawn(move || {
//             stream.write(b"
// Tectonic Shell v0.0.1
// Enter `HELP` for more options.
// >>> ").unwrap();
            handle_client(stream);
        });
    }
}