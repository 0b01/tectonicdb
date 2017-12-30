use std::path::Path;
use std::fs;
use state::*;
use dtf;
use std::io;

pub fn create_dir_if_not_exist(dtf_folder : &str) {
    if !Path::new(dtf_folder).exists() {
        fs::create_dir(dtf_folder).unwrap();
    }
}

/// Iterate through the dtf files in the folder and load some metadata into memory.
/// Create corresponding Store objects in State.
pub fn init_dbs(state: &mut State) -> Result<(), io::Error> {
    let dtf_folder = {
        let rdr = state.global.read().unwrap();
        rdr.settings.dtf_folder.clone()
    };
    for dtf_file in fs::read_dir(&dtf_folder).unwrap() {
        let fname_os = dtf_file.unwrap().file_name();
        let stem = fname_os.to_str().unwrap(); // sldjf-lks-djflk-sfsd--something.dtf
        if stem.ends_with(".dtf") {
            let basename = Path::new(&fname_os)
                       .file_stem()
                       .unwrap()
                       .to_str()
                       .unwrap(); // sldjf-lks-djflk-sfsd--something
            let full_path = &format!("{}/{}", dtf_folder, stem);
            let header_size = dtf::get_size(full_path)?;
            let symbol = dtf::read_meta(full_path)?.symbol;

            {
                let mut wtr = state.global.write().unwrap();
                // if symbol is in vec_store, append to store
                wtr.vec_store.entry(symbol.clone())
                    .and_modify(|e| e.1 += header_size)
                    .or_insert((box Vec::new(), header_size));
            }

            // insert a db store into user state
            state.store.insert(symbol.to_owned(), Store {
                name: symbol.to_owned(),
                fname: basename.to_owned(),
                in_memory: false,
                global: state.global.clone()
            });
        }
    }
    Ok(())
}

/// search every matching dtf file under folder
pub fn scan_files_for_range(folder: &str, symbol: &str, min_ts: u64, max_ts: u64) -> Result<Vec<dtf::Update>, io::Error> {
    let mut ret = Vec::new();
    match fs::read_dir(folder) {
        Err(e) => error!("Unable to read dir entries: {:?}", e),
        Ok(entries) => {
            for entry in entries {
                let entry = entry.unwrap();
                let path = entry.path();
                let fname_str = path.to_str().unwrap();

                let meta = dtf::read_meta(fname_str)?;
                if meta.symbol == symbol && within_range(
                    min_ts, max_ts, meta.min_ts, meta.max_ts) {

                    let ups = dtf::get_range_in_file(fname_str, min_ts, max_ts)?;
                    ret.extend(ups);
                }
            }
        }
    };
    Ok(ret)
}

/// check if two ranges intersect
pub fn within_range(target_min: u64, target_max: u64,
                file_min: u64, file_max: u64) -> bool {
    target_min <= file_max && target_max >= file_min
}