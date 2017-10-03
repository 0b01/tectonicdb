extern crate byteorder;
mod db;
use db::*;

mod conf;
use conf::get_config;
mod dtf;
use dtf::*;

fn db_to_update(db_ups: &Vec<OrderBookUpdate>) -> Vec<Update> {
    db_ups.iter().map(|up| Update {
        is_bid: up.is_bid as bool,
        is_trade: up.is_trade as bool,
        price: up.price as f32,
        size: up.size as f32,
        seq: up.seq as u32,
        ts: (up.ts * 1000.) as u64
    }).collect()
}


fn main() {
    let conf = get_config();
    let cxn_str : &String = conf.get("connection_string").unwrap();

    let updates : Vec<OrderBookUpdate> = db::run(&cxn_str);
    let mapped = db_to_update(&updates);
    println!("{:?}", mapped);

    let fname = "real.bin".to_owned();
    let symbol = "NEO_BTC".to_owned();
    encode(&fname, &symbol, &mapped);

    // let fname = "test.bin".to_owned();
    // let vs = decode(&fname);
    // println!("{:?}", vs);
}