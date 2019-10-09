use crate::prelude::*;
use std::path::Path;
use std::fs;
use libtectonic::dtf;
use std::borrow::Cow;

pub fn create_dir_if_not_exist(dtf_folder: &str) {
    if !Path::new(dtf_folder).exists() {
        fs::create_dir(dtf_folder).unwrap();
    }
}

/// Iterate through the dtf files in the folder and load some metadata into memory.
/// Create corresponding Store objects in State.
pub async fn init_dbs<'a>(state: &mut GlobalState) {
    let dtf_folder = state.settings.dtf_folder.clone();
    for dtf_file in fs::read_dir(&dtf_folder).unwrap() {
        let fname_os = dtf_file.unwrap().file_name();
        let stem = fname_os.to_str().unwrap(); // sldjf-lks-djflk-sfsd--something.dtf
        if stem.ends_with(".dtf") {
            let basename = Path::new(&fname_os).file_stem().unwrap().to_str().unwrap(); // sldjf-lks-djflk-sfsd--something
            let full_path = &format!("{}/{}", dtf_folder, stem);
            let header_size = match dtf::file_format::get_size(full_path) {
                Ok(size) => size,
                Err(err) => {
                    warn!("Error while retrieving size of DTF file {}: {:?}", full_path, err);
                    continue;
                }
            };
            let symbol = match dtf::file_format::read_meta(full_path) {
                Ok(meta) => meta.symbol,
                Err(err) => {
                    warn!("Error parsing metadata for DTF file {}: {:?}", full_path, err);
                    continue;
                }
            };

            // if symbol is in vec_store, append to store
            // TODO: this is not accurate at all!
            // XXX: need to keep track of file names :(
            state.books
                .entry(symbol.clone())
                .and_modify(|e| if e.nominal_count < header_size {e.nominal_count += header_size})
                .or_insert(Book::new(&symbol, state.settings.clone()));
        }
    }
}
