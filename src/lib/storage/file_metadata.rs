use storage::filetype::FileType;
use storage::DTFFileMetadata;
use serde::ser::Serialize;
use std::io;

pub trait FileMetadata: Default + Serialize {}

pub fn from_fname(fname: &str) -> Result<impl FileMetadata, io::Error> {

    let ftype = FileType::from_fname(fname);

    match ftype {
        FileType::RawDtf => DTFFileMetadata::new(fname),
    }

}
