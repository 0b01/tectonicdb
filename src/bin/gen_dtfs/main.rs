extern crate dtf;

mod db;
mod conf;

use dtf::Update;
use conf::get_config;

fn main() {
    let start_time = 1506052800;
    let end_time = 1507262400;
    let ndays = (end_time - start_time) / 3600 / 24;

    for i in 0..ndays {
        let begin_epoch = start_time + 3600 * 24 * i;
        let end_epoch = start_time + 3600 * 24 * (i+1);
        let updates = get_updates(begin_epoch, end_epoch, "btc_neo");
        println!("{:?}", updates);
    }
}

fn get_updates(begin: u32, end: u32, symbol: &str) -> Vec<Update> {
    let conf = get_config();
    let cxn_str : &String = &conf["connection_string"];
    let query = format!("
                 SELECT id, seq, is_trade, is_bid, price, size, ts
                   FROM orderbook_{}
                  WHERE ts > {} AND ts < {}
               ORDER BY id DESC
                  LIMIT 10;
    ", symbol, begin, end);
    let mut updates : Vec<Update> = db::run(&cxn_str, &query);
    updates.sort(); // important
    updates
}