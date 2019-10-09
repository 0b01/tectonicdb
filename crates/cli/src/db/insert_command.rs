use crate::dtf::update::Update;

#[derive(Clone)]
pub enum InsertCommand {
    Add(String, Update),
}

impl InsertCommand {
    pub fn into_string(self) -> Vec<String> {
        match self {
            InsertCommand::Add(dbname, up) => {
                let is_trade = if up.is_trade {"t"} else {"f"};
                let is_bid = if up.is_bid {"t"} else {"f"};
                let s = format!("ADD {}, {}, {}, {}, {}, {}; INTO {}\n",
                                up.ts, up.seq, is_trade, is_bid, up.price, up.size, dbname
                );
                vec![s]
            }
        }
    }
}

