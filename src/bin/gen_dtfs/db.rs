extern crate postgres;
use self::postgres::{Connection, TlsMode};
use dtf::Update;

pub fn run(cnx_str : &str, query : &str) -> Vec<Update> {
    let conn = Connection::connect(cnx_str.to_string(), TlsMode::None).unwrap();
    let mut v : Vec<Update> = Vec::new();

    for row in &conn.query(query, &[]).unwrap() {
        let seq : i32 = row.get(1);
        let ts : f64 = row.get(6);
        let price : f64 = row.get(4);
        let size : f64 = row.get(5);
       

        let up = Update {
            ts: (ts* 1000.) as u64,
            seq: seq as u32,
            is_trade: row.get(2),
            is_bid: row.get(3),
            price: price as f32,
            size: size as f32,
        };

        v.push(up);
    }

    v
}