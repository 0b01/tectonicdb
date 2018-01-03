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

            let mut v = entries
                .map(|entry| {
                    let entry = entry.unwrap();
                    let fname = entry.file_name();
                    let fname = fname.to_str().unwrap().to_owned();
                    let fname = &format!("{}/{}", folder, fname);
                    let meta = dtf::read_meta(fname).unwrap();
                    (fname.to_owned(), meta)
                })
                .filter(|&(ref _fname, ref meta)| {
                    meta.symbol == symbol
                    && within_range(min_ts, max_ts, meta.min_ts, meta.max_ts)
                })
                .collect::<Vec<_>>();

            // sort by min_ts
            v.sort_by(|&(ref _f0, ref m0), &(ref _f1, ref m1)| m0.cmp(m1) );

            for &(ref fname, ref _meta) in v.iter() {
                let ups = dtf::get_range_in_file(fname, min_ts, max_ts).unwrap();
                ret.extend(ups);
            }

        }
    };
    Ok(ret)
}
