use std::fmt;


/// parse symbol from string
/// from `bt_usdt_btc`
pub struct Symbol {
    pub exchange: String,
    pub currency: String,
    pub asset: String,
}

impl Symbol {
    pub fn from_str(symbol: &str) -> Option<Symbol> {
        let parts = symbol.split("_").collect::<Vec<&str>>();
        if parts.len() != 3 {
            None
        } else {
            Some(Symbol {
                exchange: parts.get(0).unwrap().to_owned().to_owned(),
                currency: parts.get(1).unwrap().to_owned().to_owned(),
                asset: parts.get(2).unwrap().to_owned().to_owned(),
            })
        }
    }
}

#[derive(Serialize)]
/// asset type:
/// spot, futures, options ...
pub enum AssetType {
    SPOT,
}

impl Default for AssetType {
    fn default() -> Self {
        AssetType::SPOT
    }
}

impl fmt::Display for AssetType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &AssetType::SPOT => 
                write!(f, "spot"),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_symbol_from_str_ok() {
        let symbol_str = "bt_usdt_btc";

        let sym = Symbol::from_str(symbol_str).unwrap();

        assert_eq!("bt", sym.exchange);
        assert_eq!("usdt", sym.currency);
        assert_eq!("btc", sym.asset);
    }
}
