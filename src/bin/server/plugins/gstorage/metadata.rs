const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

use plugins::gstorage::serde_json::from_str;
use libtectonic::storage::file_metadata::FileMetadata;

use std::fmt;
use std::error;

#[derive(Serialize)]
enum GStorageOp {
    AddDtf,
}

impl fmt::Display for GStorageOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &GStorageOp::AddDtf => 
                write!(f, "add.dtf"),
        }
    }
}


//------------------------------------------------

#[derive(Serialize)]
pub struct GStorageOpMetadata {

    /*-------------- returned vals -------------*/
    id: String,
    #[serde(rename="selfLink")]
    self_link: String,
    name: String,
    bucket: String,
    metageneration: String,

    #[serde(rename="timeCreated")]
    time_created: String,

    #[serde(rename="timeStorageClassUpdated")]
    time_storage_class_updated: String,

    size: String,

    #[serde(rename="md5Hash")]
    md5_hash: String,
    #[serde(rename="mediaLink")]
    media_link: String,

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
            op_type: GStorageOp::AddDtf,

            chunked: false,
            n_batch_parts: 1,
            x_of_n: 1,

            status: "ok".to_owned(),
            dtf_spec: "v1".to_owned(),
            priority: 0,
            client_version: VERSION.unwrap_or("unknown").to_owned(),
            server_version: "?".to_owned(),
            _prefix: "".to_owned(),

            batch_hash: "".to_owned(),

            response_time: 0,
            time_storage_class_updated: "".to_owned(),
            size: "".to_owned(),
            time_created: "".to_owned(),

            start_ts: 0,
            finish_ts: 0,
            bucket: "".to_owned(),
            id: "".to_owned(),
            self_link: "".to_owned(),
            name: "".to_owned(),
            metageneration: "".to_owned(),
            md5_hash: "".to_owned(),
            media_link: "".to_owned(),

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
        meta.self_link = resp.self_link;
        meta.name = resp.name;
        meta.bucket = resp.bucket;
        meta.metageneration = resp.metageneration;
        meta.time_created = resp.time_created;
        meta.time_storage_class_updated = resp.time_storage_class_updated;
        meta.size = resp.size;
        meta.md5_hash = resp.md5_hash.clone();
        meta.media_link = resp.media_link;

        meta.batch_hash = resp.md5_hash;

        Ok(meta)
    }
}

#[derive(Deserialize)]
struct GStorageResp {
    id: String,
    #[serde(rename="selfLink")]
    self_link: String,
    name: String,
    bucket: String,
    metageneration: String,
    #[serde(rename="timeCreated")]
    time_created: String,
    #[serde(rename="timeStorageClassUpdated")]
    time_storage_class_updated: String,
    size: String,
    #[serde(rename="md5Hash")]
    md5_hash: String,
    #[serde(rename="mediaLink")]
    media_link: String,
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

