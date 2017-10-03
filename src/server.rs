use std::net::TcpStream;
use std::net::TcpListener;
use std::thread;

// traits
use std::io::{Read, Write};

pub fn handle_client(mut stream: TcpStream) {
    let mut buf;
    loop {
        // clear out the buffer so we don't send garbage
        buf = [0; 512];
        let _ = match stream.read(&mut buf) {
            Err(e) => panic!("Got an error: {}", e),
            Ok(m) => {
                if m == 0 {
                    // we've got an EOF
                    break;
                }
                m
            },
        };

        println!("GOT: {:?}", "1");

        match stream.write(&buf) {
            Err(_) => break,
            Ok(_) => continue,
        }
    }
}


pub fn run_server(){
    let listener = TcpListener::bind("127.0.0.1:9001").unwrap();
    for stream in listener.incoming() {
        match stream {
            Err(e) => { println!("failed: {}", e) }
            Ok(stream) => {
                thread::spawn(move || {
                    handle_client(stream)
                });
            }
        }
    }
}