use storage::filetype::FileType;
use storage::DTFFileMetadata;
use serde::ser::Serialize;

pub trait FileMetadata: Default + Serialize { }

pub fn from_fname(fname: &str) -> impl FileMetadata {

    let ftype = FileType::from_fname(fname);

    match ftype {
        FileType::RAW_DTF => DTFFileMetadata::new(fname)
    }

}