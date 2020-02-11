use std::{io, fs};
/// print meta data about folder containing dtf files
pub fn print_total_folder_updates_len(folder: &str) -> Result<u64, io::Error> {
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ))
        }
        Ok(entries) => {
            let mut sum = 0;
            for entry in entries {
                let e = if entry.is_err() {continue} else {entry.unwrap()};
                if e.path().is_file() && e.path().extension().unwrap() == "dtf" {

                    let fname = e.file_name();
                    let fname = fname.to_str().unwrap().to_owned();
                    let fname = &format!("{}/{}", folder, fname);
                    let meta = crate::dtf::file_format::read_meta(fname).unwrap();

                    println!("---- Filename: {} ----", fname);
                    println!("Metadata: {:#?}", meta);
                    sum += meta.count;
                }
            }
            Ok(sum)
        }
    }
}