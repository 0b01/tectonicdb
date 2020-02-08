use crate::storage::filetype::FileType;
use crate::storage::dtf_file_metadata::DTFFileMetadata;
use serde::ser::Serialize;
use std::io;

/// Marker trait indicating MetaData structs
pub trait FileMetadata: Default + Serialize {}

/// Get file metadata from filiename
pub fn from_fname(fname: &str) -> Result<impl FileMetadata, io::Error> {
    let ftype = FileType::from_fname(fname);

    match ftype {
        FileType::RawDtf => DTFFileMetadata::new(fname),
    }
}
