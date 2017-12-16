use std::fmt;
use dtf::read_magic_value;
use std::io::{
    BufReader,
};
use std::fs::File;

pub enum FileType {
    RAW_DTF,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &FileType::RAW_DTF => 
                write!(f, "raw.dtf"),
        }
    }
}

impl Default for FileType {
    fn default() -> Self {
        FileType::RAW_DTF
    }
}

impl FileType {
    pub fn from_fname(fname: &str) -> FileType {

        let file = File::open(fname).expect("OPENING FILE");
        let mut rdr = BufReader::new(file);

        if read_magic_value(&mut rdr) {
            return FileType::RAW_DTF;
        }

        unimplemented!()
    }
}