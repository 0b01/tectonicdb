use FileType;
use dtf::{self, Symbol, AssetType};
use file_metadata::FileMetadata;
use std::io;
use std::fs;


#[derive(Default, Serialize)]
pub struct DTFFileMetadata {
    file_type: FileType,

    file_size: u64, // in byte
    exchange: String,
    currency: String,
    asset: String,
    asset_type: AssetType,
    first_epoch: u64,
    last_epoch: u64,
    total_updates: u64,
    assert_continuity: bool,
    discontinuities: Vec<(u64, u64)>, // (start, finish)
    continuation_candles: bool,

    uuid: String,
    filename: String,

    tags: Vec<String>,
    errors: Vec<String>,
}

impl FileMetadata for DTFFileMetadata {}

impl DTFFileMetadata {
    pub fn new(fname: &str) -> Result<DTFFileMetadata, io::Error> {
        let metadata: dtf::Metadata = dtf::read_meta(fname);
        let file_size = fs::metadata(fname)?.len();
        let symbol = match Symbol::from_str(&metadata.symbol) {
            Some(sym) => sym,
            None => {
                return Err(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unable to parse symbol {}", metadata.symbol)));
            }
        };
        let first_epoch = metadata.min_ts;
        let last_epoch = metadata.max_ts;
        let total_updates = metadata.nums;

        Ok(DTFFileMetadata {
            file_type: FileType::RawDtf,
            file_size,
            exchange: symbol.exchange,
            currency: symbol.currency,
            asset: symbol.asset,
            asset_type: AssetType::SPOT,
            first_epoch,
            last_epoch,
            total_updates,

            assert_continuity: true,
            discontinuities: vec![],
            continuation_candles: false,

            filename: fname.to_owned(),

            ..Default::default()

            // uuid: 
            // tags: 
            // errors: 
        })
    }
}