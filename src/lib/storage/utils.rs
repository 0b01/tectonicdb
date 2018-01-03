use std::{io, fs};

use dtf;
use utils::within_range;

/// search every matching dtf file under folder
pub fn scan_files_for_range(
    folder: &str,
    symbol: &str,
    min_ts: u64,
    max_ts: u64,
) -> Result<Vec<dtf::Update>, io::Error> {
    let mut ret = Vec::new();
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ))
        }
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                let fname_str = path.to_str().unwrap();

                let meta = dtf::read_meta(fname_str)?;

                if meta.symbol == symbol && within_range(min_ts, max_ts, meta.min_ts, meta.max_ts) {

                    let ups = dtf::get_range_in_file(fname_str, min_ts, max_ts)?;
                    ret.extend(ups);
                }
            }
        }
    };
    Ok(ret)
}
