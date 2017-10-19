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
        let fname_os = dtf_file.unwrap().file_name();
        let fname = fname_os.to_str().unwrap(); // something.dtf
        if fname.ends_with(".dtf") {
            let name = Path::new(&fname_os)
                       .file_stem() // (.*).dtf
                       .unwrap()
                       .to_str()
                       .unwrap();
            let full_path = &format!("{}/{}", dtf_folder, fname);
            let header_size = dtf::get_size(full_path);

            // insert a vector into shared hashmap
            {
                let mut global = state.global.write().unwrap();
                global.vec_store.insert(name.to_owned(), Vec::new());
            }

            // insert a db store into user state
            state.store.insert(name.to_owned(), Store {
                name: name.to_owned(),
                size: header_size,
                in_memory: false,
                global: state.global.clone()
            });
        }
    }
}