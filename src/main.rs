extern crate byteorder;
mod db;
use db::*;

mod conf;
use conf::get_config;
mod dtf;
use dtf::*;
fn main() {

    let conf = get_config();
    let cxn_str : &String = conf.get("connection_string").unwrap();

    let updates : Vec<OrderBookUpdate> = db::run(&cxn_str);
    println!("{:?}", updates);

    let fname = "test.bin".to_owned();
    let vs = decode(&fname);
    println!("{:?}", vs);
}