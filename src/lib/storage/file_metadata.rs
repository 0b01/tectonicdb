use storage::filetype::FileType;
use storage::DTFFileMetadata;

pub trait FileMetadata: Default { } // marker trait

pub fn from_fname(fname: &str) -> impl FileMetadata {

    let ftype = FileType::from_fname(fname);

    match ftype {
        FileType::RAW_DTF => DTFFileMetadata::new(fname)
    }

}