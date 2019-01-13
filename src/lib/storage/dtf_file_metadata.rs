use std::env;
use std::fs;
use std::io;

use crate::dtf::file_format::{Metadata, read_meta};
use crate::dtf::symbol::{ Symbol, AssetType };
use crate::storage::{
    filetype::FileType,
    file_metadata::FileMetadata,
};

fn key_or_default(key: &str, default: &str) -> String {
   match env::var(key) {
        Ok(val) => val,
        Err(_) => default.into(),
    }
}

fn parse_dtf_metadata_tags() -> Vec<String> {
    key_or_default("DTF_METADATA_TAGS", "")
        .split(',')
        .map(String::from)
        .collect()
}

use uuid::Uuid;

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

    uuid: Uuid,
    filename: String,

    tags: Vec<String>,
    errors: Vec<String>,
}

impl FileMetadata for DTFFileMetadata {}

impl DTFFileMetadata {
    pub fn new(fname: &str) -> Result<DTFFileMetadata, io::Error> {
        let metadata: Metadata = read_meta(fname)?;
        let file_size = fs::metadata(fname)?.len();
        let symbol = match Symbol::from_str(&metadata.symbol) {
            Some(sym) => sym,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unable to parse symbol {}", metadata.symbol),
                ));
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
            tags: parse_dtf_metadata_tags(),

            ..Default::default() // uuid:
                                 // errors:
        })
    }
}

#[test]
fn dtf_metadata_tags_parsing() {
    let sample_env = "foo,bar,key:value,test2";
    let parsed: Vec<String> = sample_env
        .split(',')
        .map(String::from)
        .collect();

    let expected: Vec<String> = ["foo", "bar", "key:value", "test2"]
        .into_iter()
        .map(|s| String::from(*s))
        .collect();

    assert_eq!(parsed, expected);
}
