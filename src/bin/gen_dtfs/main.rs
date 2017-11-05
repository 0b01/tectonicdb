extern crate dtf;
extern crate postgres;
use self::postgres::{Connection, TlsMode};

mod db;
mod conf;

use dtf::Update;
use conf::get_config;
use std::path::Path;

// let begin_epoch = start_time + 3600 * 24 * i;
// let end_epoch = start_time + 3600 * 24 * (i+1);
static MILLION: u32 = 1_000_000;


fn create_or_append(fname: &str, ups: Vec<Update>) {
    let fullname = format!("old/{}", &fname);
    if Path::new(&fullname).exists() {
        dtf::append(&fullname, &ups);
    } else {
        dtf::encode(&fullname, fname.clone(), &ups);
    }
}

fn pretty_name(dbname : &str) -> String {
    dbname.replace("public.orderbook_", "")
}

fn main() {
    let names = get_names();
    for dbname in names {
        let name = pretty_name(dbname.clone().as_str());
        let max = get_max(dbname.clone().as_str());

        println!("Name: {} Total: {}", name, max);

        let truncated_n = (max as u32).checked_div(MILLION).unwrap();
        for i in 0..truncated_n {
            let begin_id = i * MILLION;
            let end_id = (i+1) * MILLION;
            let updates = get_updates(begin_id, end_id, dbname.clone().as_str());
            create_or_append(&name, updates);
            println!("Current: {} to {}", begin_id, end_id);
        }
        let begin_id = truncated_n * MILLION;
        let end_id = max as u32 + 1;
        let updates = get_updates(begin_id, end_id, dbname.clone().as_str());
        create_or_append(name.clone().as_str(), updates);

        println!("DONE");
    }
}

fn get_max(symbol: &str) -> i32 {
    let conf = get_config();
    let cxn_str : &str = &conf["connection_string"];
    let query = format!("SELECT MAX(id) FROM {}", symbol);
    let conn = Connection::connect(cxn_str, TlsMode::None).unwrap();
    let row = conn.query(&query, &[]).unwrap();
    let res : i32 = row.get(0).get(0);
    res
}

fn get_names() -> Vec<String> {
    let conf = get_config();
    let cxn_str : &str = &conf["connection_string"];
    let query = "SELECT
    table_schema || '.' || table_name
FROM
    information_schema.tables
WHERE
    table_type = 'BASE TABLE'
AND
    table_schema NOT IN ('pg_catalog', 'information_schema');
";
    let conn = Connection::connect(cxn_str, TlsMode::None).unwrap();
    let rows = conn.query(&query, &[]).unwrap();
    let mut vs = Vec::new();
    for row in &rows {
        let name: String = row.get(0);
        vs.push(name);
    }

    vs.into_iter()
      .filter(|x| !x.contains("snapshot"))
      .collect()
}

fn get_updates(begin: u32, end: u32, symbol: &str) -> Vec<Update> {
    let conf = get_config();
    let cxn_str : &String = &conf["connection_string"];
    let query = format!("
                 SELECT id, seq, is_trade, is_bid, price, size, ts
                   FROM {}
                  WHERE id > {} AND id < {}
               ORDER BY id DESC
                  LIMIT 1000000;
    ", symbol, begin, end);
    let mut updates : Vec<Update> = db::run(&cxn_str, &query);
    updates.sort(); // important
    updates
}