use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, BufWriter, Write, BufRead};


pub fn start_server() {
    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();

    fn handle_client(stream: TcpStream) {
        println!("Client connected");

        let mut writer = BufWriter::new(&stream);
        writer.write_all("Red\n".as_bytes()).expect("could not write");
        writer.flush().expect("could not flush");

        let mut reader = BufReader::new(&stream);
        let mut response = String::new();
        reader.read_line(&mut response).expect("could not read");
        println!("Server received {}", response);
    }

    for stream in listener.incoming() {
        let stream = stream.expect("Unable to accept");
        handle_client(stream);
    }
}
