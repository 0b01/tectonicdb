extern crate byteorder;

mod db;
mod conf;
mod dtf;
mod server;

use db::*;
use conf::get_config;
use dtf::*;
use server::*;



fn main() {
    // let conf = get_config();
    // let cxn_str : &String = &conf["connection_string"];
    // let updates : Vec<OrderBookUpdate> = db::run(&cxn_str);
    // let mut mapped : Vec<Update> = updates.iter().map(|d| d.to_update()).collect();
    // mapped.sort(); // important
    // // println!("{:?}", mapped);
    // let fname = "real.bin".to_owned();
    // let symbol = "NEO_BTC".to_owned();
    // encode(&fname, &symbol, &mut mapped);

    
    run_server();


}
