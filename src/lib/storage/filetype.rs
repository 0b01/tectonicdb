use dtf::read_magic_value;
use std::io::{
    BufReader,
};
use std::fs::File;

#[derive(Serialize)]
pub enum FileType {
    RawDtf,
}

impl Default for FileType {
    fn default() -> Self {
        FileType::RawDtf
    }
}

impl FileType {
    pub fn from_fname(fname: &str) -> FileType {

        let file = File::open(fname).expect("OPENING FILE");
        let mut rdr = BufReader::new(file);

        if read_magic_value(&mut rdr) {
            return FileType::RawDtf;
        }

        unimplemented!()
    }
}