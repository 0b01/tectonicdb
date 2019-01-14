use std::str::FromStr;
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

/// Data structure for storing metadata for dtf files
#[derive(Default, Serialize)]
pub struct DTFFileMetadata {
    /// file type is dtf
    pub file_type: FileType,

    /// File size on disk
    pub file_size: u64, // in byte
    /// Which exchange is the data from
    pub exchange: String,
    /// Name of currency
    pub currency: String,
    /// Name of asset
    pub asset: String,
    /// Type of asset: Spot, Future, other derivatives
    pub asset_type: AssetType,
    /// Timestamp of the first update
    pub first_epoch: u64,
    /// Timestamp of the Last update
    pub last_epoch: u64,
    /// Number of updates in the file
    pub total_updates: u64,
    /// Is the data continuous, currently it's always true
    pub assert_continuity: bool,
    /// Discrete jumps in data, currently unimplemented!()
    pub discontinuities: Vec<(u64, u64)>, // (start, finish)
    /// If there are continuation candles
    pub continuation_candles: bool,

    /// Unique ID of the file
    pub uuid: Uuid,
    /// Filename
    pub filename: String,

    /// Tags
    pub tags: Vec<String>,
    /// Errors in file
    pub errors: Vec<String>,
}

impl FileMetadata for DTFFileMetadata {}

impl DTFFileMetadata {
    /// Read dtf file metadata
    pub fn new(fname: &str) -> Result<DTFFileMetadata, io::Error> {
        let metadata: Metadata = read_meta(fname)?;
        let file_size = fs::metadata(fname)?.len();
        let symbol = match Symbol::from_str(&metadata.symbol) {
            Ok(sym) => sym,
            Err(()) => {
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
