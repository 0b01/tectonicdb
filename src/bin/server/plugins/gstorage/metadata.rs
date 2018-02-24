const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

use plugins::gstorage::serde_json::from_str;
use libtectonic::storage::file_metadata::FileMetadata;

use std::fmt;
use std::error;
use std::borrow::Cow;

#[derive(Serialize)]
enum GStorageOp {
    AddDtf,
}

impl fmt::Display for GStorageOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &GStorageOp::AddDtf => write!(f, "add.dtf"),
        }
    }
}


//------------------------------------------------

#[derive(Serialize)]
pub struct GStorageOpMetadata<'a> {
    /*-------------- returned vals -------------*/
    id: Cow<'a, str>,
    #[serde(rename = "selfLink")]
    self_link: Cow<'a, str>,
    name: Cow<'a, str>,
    bucket: Cow<'a, str>,
    metageneration: Cow<'a, str>,

    #[serde(rename = "timeCreated")]
    time_created: Cow<'a, str>,

    #[serde(rename = "timeStorageClassUpdated")]
    time_storage_class_updated: Cow<'a, str>,

    size: Cow<'a, str>,

    #[serde(rename = "md5Hash")]
    md5_hash: Cow<'a, str>,
    #[serde(rename = "mediaLink")]
    media_link: Cow<'a, str>,

    /*-------------- operation -------------*/
    op_type: GStorageOp,
    pub start_ts: u32,
    pub finish_ts: u32,
    pub response_time: u32,

    /*-------------- batch -------------*/
    chunked: bool,
    n_batch_parts: u8,
    x_of_n: u8,
    batch_hash: Cow<'a, str>,

    /*-------------- misc -------------*/
    status: Cow<'a, str>,
    dtf_spec: Cow<'a, str>,
    priority: u16,
    client_version: Cow<'a, str>,
    server_version: Cow<'a, str>,
    _prefix: Cow<'a, str>,
}

impl<'a> Default for GStorageOpMetadata<'a> {
    fn default() -> Self {
        GStorageOpMetadata {
            op_type: GStorageOp::AddDtf,

            chunked: false,
            n_batch_parts: 1,
            x_of_n: 1,

            status: "ok".into(),
            dtf_spec: "v1".into(),
            priority: 0,
            client_version: VERSION.unwrap_or("unknown").into(),
            server_version: "?".into(),
            _prefix: "".into(),

            batch_hash: "".into(),

            response_time: 0,
            time_storage_class_updated: "".into(),
            size: "".into(),
            time_created: "".into(),

            start_ts: 0,
            finish_ts: 0,
            bucket: "".into(),
            id: "".into(),
            self_link: "".into(),
            name: "".into(),
            metageneration: "".into(),
            md5_hash: "".into(),
            media_link: "".into(),

            // ..Default::default()
        }
    }
}

impl<'a> GStorageOpMetadata<'a> {
    pub fn new(
        resp: String,
        start_ts: u32,
        finish_ts: u32,
    ) -> Result<GStorageOpMetadata<'a>, Box<error::Error>> {

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
struct GStorageResp<'a> {
    id: Cow<'a, str>,
    #[serde(rename = "selfLink")]
    self_link: Cow<'a, str>,
    name: Cow<'a, str>,
    bucket: Cow<'a, str>,
    metageneration: Cow<'a, str>,
    #[serde(rename = "timeCreated")]
    time_created: Cow<'a, str>,
    #[serde(rename = "timeStorageClassUpdated")]
    time_storage_class_updated: Cow<'a, str>,
    size: Cow<'a, str>,
    #[serde(rename = "md5Hash")]
    md5_hash: Cow<'a, str>,
    #[serde(rename = "mediaLink")]
    media_link: Cow<'a, str>,
}


//----------------------------------------------

#[derive(Serialize)]
pub struct GStorageMetadata<'a, T: FileMetadata> {
    // meta section: about storage operation
    meta: GStorageOpMetadata<'a>,
    // data section: about the file itself
    data: T,
}

impl<'a, T: FileMetadata> GStorageMetadata<'a, T> {
    pub fn new(op_meta: GStorageOpMetadata<'a>, file_meta: T) -> GStorageMetadata<'a, T> {
        GStorageMetadata {
            meta: op_meta,
            data: file_meta,
        }
    }
}

impl<'a, T: FileMetadata> Default for GStorageMetadata<'a, T> {
    fn default() -> Self {
        GStorageMetadata {
            meta: Default::default(),
            data: Default::default(),
        }
    }
}
