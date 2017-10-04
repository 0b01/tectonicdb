/// Server should handle requests similar to Redis
/// 
/// PING
/// 
/// DB neo_btc
/// 
/// ADD ts, seq, is_trade, bool, price, size
/// 
/// GET neo_btc 2017-06-10 TO 2018-09-20 LIMIT 100
/// 
/// BULKADD
/// ts, seq, is_trade, bool, price, size
/// ts, seq, is_trade, bool, price, size
/// DDAKLUB
/// 


use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;
use std::str;

struct State {
    db: String
}

fn gen_response(string : &str, state: &mut State) -> String {
    if string == "" {
        "".to_owned()
    } else if string == "PING" {
        "+PONG.\n".to_owned()

    } else if string.starts_with("DB") {
        let dbname : &str = &string[3..];
        state.db = dbname.to_owned();
        format!("+SWITCHED TO DB `{}`.\n", &dbname)
    } else if string.starts_with("ADD") {
        "".to_owned()
    } else if string.starts_with("GET") {
        "".to_owned()
    } else if string.starts_with("BULKADD") {
        "".to_owned()
    } else {
        format!("CURRENT DB: {} -ERR unknown command '{}'.\n", state.db ,&string)
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];
    let mut state = State {
        db: "".to_owned(),
    };

    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        if bytes_read == 0 { break }
        let ping = str::from_utf8(&buf[..(bytes_read-1)]).unwrap();

        let resp = gen_response(&ping, &mut state);
        stream.write(resp.as_bytes()).unwrap();
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