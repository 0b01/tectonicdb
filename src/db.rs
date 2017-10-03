extern crate postgres;
use self::postgres::{Connection, TlsMode};


#[derive(Debug)]
pub struct OrderBookUpdate {
    pub id: i32,
    pub seq: i32,
    pub is_trade: bool,
    pub is_bid: bool,
    pub price: f64,
    pub size: f64,
    pub ts: f64
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
            // -- trade_id: row.get(7)  -- always null,
            // -- order_type: row.get(8)  -- sometimes null
        };
        // println!("{:?}", up);
        v.push(up);
    }

    v
}