use crate::dtf::file_format::read_magic_value;
use std::io::BufReader;
use std::fs::File;
use csv::{DeserializeRecordsIntoIter, ReaderBuilder};
use std::path::Path;
use crate::dtf::{
    update::Update,
    file_format::{append, encode},
};

/// File types for storing financial data, currently there's only RawDtf
#[derive(Serialize)]
pub enum FileType {
    /// Dense Tick Format bytes
    RawDtf,
}

impl Default for FileType {
    fn default() -> Self {
        FileType::RawDtf
    }
}

impl FileType {
    /// Get file type from file
    pub fn from_fname(fname: &str) -> FileType {

        let file = File::open(fname).expect("OPENING FILE");
        let mut rdr = BufReader::new(file);

        if read_magic_value(&mut rdr).unwrap() {
            return FileType::RawDtf;
        }

        unreachable!()
    }
}

/// ```csv
/// id,exchange,symbol,date,price,amount,sell
/// 109797481,be,dashbtc,1498694478000,0.07154,0.40495999,false
/// ```
#[derive(Deserialize)]
struct KaikoCsvEntry {
    pub id: String,
    pub exchange: String,
    pub symbol: String,
    pub date: u64,
    pub price: f32,
    pub amount: f32,
    pub sell: Option<bool>,
}

impl Into<Update> for KaikoCsvEntry {
    fn into(self) -> Update {
        Update {
            ts: self.date,
            seq: self.id.parse().unwrap_or(0),
            is_trade: true,
            is_bid: !self.sell.unwrap_or(false),
            price: self.price,
            size: self.amount,
        }
    }
}

pub(crate) fn parse_kaiko_csv_to_dtf_inner(symbol: &str, filename: &str, csv_str: &str) -> Option<String> {
    let csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_str.as_bytes());

    // Parse the full CSV into a vector of `KaikoCsvEntry`s and make into `Update`s
    let iter: DeserializeRecordsIntoIter<_, KaikoCsvEntry> = csv_reader.into_deserialize();
    let size_hint = iter.size_hint().0;
    let mut updates: Vec<Update> = Vec::with_capacity(size_hint);

    for kaiko_entry_res in iter {
        match kaiko_entry_res {
            Ok(kaiko_entry) => updates.push(kaiko_entry.into()),
            Err(err) => { return Some(format!("{:?}", err)); }
        }
    }

    // Write or append the updates into the target DTF file
    let fpath = Path::new(&filename);
    let res = if fpath.exists() {
        append(filename, &updates)
    } else {
        encode(filename, symbol, &updates)
    };

    match res {
        Ok(_) => None,
        Err(err) => Some(format!("Error writing DTF to output file: {:?}", err)),
    }
}