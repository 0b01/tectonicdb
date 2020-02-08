use std::{io, fs};
/// Get total number of updates from all files in a folder
pub fn total_folder_updates_len(folder: &str) -> Result<usize, io::Error> {
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ))
        }
        Ok(entries) => {
            let count = entries
                .map(|entry| {
                    let entry = entry.unwrap();
                    let fname = entry.file_name();
                    let fname = fname.to_str().unwrap().to_owned();
                    let fname = &format!("{}/{}", folder, fname);
                    let meta = crate::dtf::file_format::read_meta(fname).unwrap();
                    meta.count as usize
                })
                .sum();

            Ok(count)
        }
    }
}