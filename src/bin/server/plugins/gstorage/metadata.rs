use dtf::storage::DTFFileMetadata;
use plugins::gstorage::serde_json::Value;
use plugins::gstorage::serde::ser::Serialize;
use plugins::gstorage::serde_json::from_str;
use plugins::gstorage::serde::de::Deserialize;
use dtf::file_metadata::FileMetadata;

use std::fmt;
use std::error;

#[derive(Serialize)]
enum GStorageOp {
    ADD_DTF,
}

impl fmt::Display for GStorageOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &GStorageOp::ADD_DTF => 
                write!(f, "add.dtf"),
        }
    }
}


//------------------------------------------------

#[derive(Serialize)]
pub struct GStorageOpMetadata {

    /*-------------- returned vals -------------*/
    id: String,
    selfLink: String,
    name: String,
    bucket: String,
    metageneration: String,
    timeCreated: String,
    timeStorageClassUpdated: String,
    size: String,
    md5Hash: String,
    mediaLink: String,

    /*-------------- operation -------------*/
    op_type: GStorageOp,
    pub start_ts: u32,
    pub finish_ts: u32,
    pub response_time: u32,

    /*-------------- batch -------------*/
    chunked: bool,
    n_batch_parts: u8,
    x_of_n: u8,
    batch_hash: String,


    /*-------------- misc -------------*/
    status: String,
    dtf_spec: String,
    priority: u16,
    client_version: String,
    server_version: String,
    _prefix: String,

}

impl Default for GStorageOpMetadata {
    fn default() -> Self {
        GStorageOpMetadata {
            op_type: GStorageOp::ADD_DTF,

            chunked: false,
            n_batch_parts: 1,
            x_of_n: 1,

            status: "ok".to_owned(),
            dtf_spec: "v1".to_owned(),
            priority: 0,
            client_version: "0.1.7".to_owned(),
            server_version: "?".to_owned(),
            _prefix: "".to_owned(),

            batch_hash: "".to_owned(),

            response_time: 0,
            timeStorageClassUpdated: "".to_owned(),
            size: "".to_owned(),
            timeCreated: "".to_owned(),

            start_ts: 0,
            finish_ts: 0,
            bucket: "".to_owned(),
            id: "".to_owned(),
            selfLink: "".to_owned(),
            name: "".to_owned(),
            metageneration: "".to_owned(),
            md5Hash: "".to_owned(),
            mediaLink: "".to_owned(),

            // ..Default::default()
        }
    }
}

impl GStorageOpMetadata {
    pub fn new(resp: String, start_ts: u32, finish_ts: u32) -> Result<GStorageOpMetadata, Box<error::Error>> {

        let mut meta = GStorageOpMetadata::default();

        let resp = from_str::<GStorageResp>(&resp)?;
        meta.start_ts = start_ts;
        meta.finish_ts = finish_ts;
        meta.response_time = finish_ts - start_ts;
        meta.id = resp.id;
        meta.selfLink = resp.selfLink;
        meta.name = resp.name;
        meta.bucket = resp.bucket;
        meta.metageneration = resp.metageneration;
        meta.timeCreated = resp.timeCreated;
        meta.timeStorageClassUpdated = resp.timeStorageClassUpdated;
        meta.size = resp.size;
        meta.md5Hash = resp.md5Hash.clone();
        meta.mediaLink = resp.mediaLink;

        meta.batch_hash = resp.md5Hash;

        Ok(meta)
    }
}

#[derive(Deserialize)]
struct GStorageResp {
    kind: String,
    id: String,
    selfLink: String,
    name: String,
    bucket: String,
    generation: String,
    metageneration: String,
    timeCreated: String,
    updated: String,
    storageClass: String,
    timeStorageClassUpdated: String,
    size: String,
    md5Hash: String,
    mediaLink: String,
    crc32c: String,
    etag: String,
}


//----------------------------------------------

#[derive(Serialize)]
pub struct GStorageMetadata<T: FileMetadata> {
    // meta section: about storage operation
    meta: GStorageOpMetadata,
    // data section: about the file itself
    data: T

}

impl<T: FileMetadata> GStorageMetadata<T> {
    pub fn new(op_meta: GStorageOpMetadata, file_meta: T) -> GStorageMetadata<T> {
        GStorageMetadata {
            meta: op_meta,
            data: file_meta
        }
    }
}

impl<T: FileMetadata> Default for GStorageMetadata<T> {
    fn default() -> Self {
        GStorageMetadata {
            meta: Default::default(),
            data: Default::default(),
        }
    }
}

