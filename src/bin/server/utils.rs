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
pub fn init_dbs(dtf_folder : &str, state: &mut State) {
    for dtf_file in fs::read_dir(&dtf_folder).unwrap() {
        let dtf_file = dtf_file.unwrap();
        let fname_os = dtf_file.file_name();
        let fname = fname_os.to_str().unwrap();
        if fname.ends_with(".dtf") {
            let name = Path::new(&fname_os).file_stem().unwrap().to_str().unwrap();
            let header_size = dtf::get_size(&format!("{}/{}", dtf_folder, fname));
            state.store.insert(name.to_owned(), Store {
                folder: dtf_folder.to_owned(),
                name: name.to_owned(),
                v: Vec::new(),
                size: header_size,
                in_memory: false

            });
        }
    }
}