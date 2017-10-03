extern crate postgres;
use self::postgres::{Connection, TlsMode};


#[derive(Debug)]
pub struct OrderBookUpdate {
    id: i32,
    seq: i32,
    is_trade: bool,
    is_bid: bool,
    price: f64,
    size: f64,
    ts: f64,
    order_type: i32
}

pub fn run(cnx_str : &String) -> Vec<OrderBookUpdate> {
    let conn = Connection::connect(cnx_str.to_string(), TlsMode::None).unwrap();
    let mut v : Vec<OrderBookUpdate> = Vec::new();

    for row in &conn.query("select * FROM orderbook_btc_neo ORDER BY id DESC LIMIT 1;", &[]).unwrap() {
        let up = OrderBookUpdate {
            id: row.get(0),
            seq: row.get(1),
            is_trade: row.get(2),
            is_bid: row.get(3),
            price: row.get(4),
            size: row.get(5),
            ts: row.get(6),
            // -- trade_id: null,
            order_type: row.get(8)
        };
        // println!("{:?}", up);
        v.push(up);
    }

    v
}