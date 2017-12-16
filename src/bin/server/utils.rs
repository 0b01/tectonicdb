use std::path::Path;
use std::fs;
use state::*;
use dtf;

pub fn create_dir_if_not_exist(dtf_folder : &str) {
    if !Path::new(dtf_folder).exists() {
        fs::create_dir(dtf_folder).unwrap();
    }
}

/// Iterate through the dtf files in the folder and load some metadata into memory.
/// Create corresponding Store objects in State.
pub fn init_dbs(state: &mut State) {
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
            let header_size = dtf::get_size(full_path);
            let symbol = dtf::read_meta(full_path).symbol;

            {
                let rdr = state.global.read().unwrap();
                if rdr.vec_store.contains_key(&symbol) {
                    return;
                }
            }

            // insert a vector into shared hashmap
            {
                let mut global = state.global.write().unwrap();
                global.vec_store.insert(symbol.to_owned(), (Vec::new(), header_size));
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
}