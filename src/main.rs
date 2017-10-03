extern crate byteorder;
mod db;
use db::*;

mod conf;
use conf::get_config;

mod dtf;
use dtf::*;

mod server;
use server::*;

use std::thread;
use std::net::{TcpListener, TcpStream};
use std::io::{BufReader, BufWriter, Write, BufRead};


fn main() {
    let conf = get_config();
    let cxn_str : &String = &conf["connection_string"];


    let updates : Vec<OrderBookUpdate> = db::run(&cxn_str);
    let mut mapped : Vec<Update> = updates.iter().map(|d| d.to_update()).collect();

    println!("{:?}", mapped);

    let fname = "real.bin".to_owned();
    let symbol = "NEO_BTC".to_owned();
    encode(&fname, &symbol, &mut mapped);



    // // server
    // thread::spawn(start_server);

    // let player_stream = TcpStream::connect("127.0.0.1:8000").expect("Couldn't connect");

    // let mut reader = BufReader::new(&player_stream);
    // let mut response = String::new();
    // reader.read_line(&mut response).expect("Could not read");
    // println!("Player received >{}<", response.trim());

    // let mut writer = BufWriter::new(&player_stream);
    // writer.write_all("NAME\n".as_bytes()).expect("Could not write");
}