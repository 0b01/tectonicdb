/// {
///     // metadata describes the process of upload
///     "meta": {
///         "type": "add.dtf", /* there may be several operations sent to the data collection backend */
///         "upload_start_ts": 1512435389000, /* epoch in ms, time to start upload */
///         "upload_finish_ts": 1512435389100, /* time upload is done */
///         "response_time": 100, /* ms, time to upload the file start to finish */
///         "request_size": 1000, /* bytes size of this request */

///         // a record may be chunked into several parts,
///         // send one metadata request for each uploaded part
///         // server should be this many requests
///         "batch_parts": 3, /* total parts */
///         "x_of_n": 1, /* 1-indexed, 1 of 3 etc, 1 of 1 */

///         // the hash of this request sha256(uuid)
///         "request_hash": "4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865", // sha256sum
///         // the hash of this batch (same for all parts)
///         "batch_hash": "fdf3cfdb724f7ed282cb4f7e34349c05aaaa8125bc51daeca3456ce6646c3586",

///         "status": "ok", /* sanity check, this field must be ok */
///         "dtf_spec": "v0.1", /* file spec version */
///         "priority": 100, /* 1 to 100, save high priority files first */
///         "client_version": "1.10.1", /* version of sender of the request */
///         "server_version": "10.1.1", /* version of server to ingest the request */
///         "_prefix": "", /* optional field for future emergency use */
///     },

///     // this section is about the file itself
///     "data": {
///         "type": "raw.dtf", // candles, trades, npy
///         "file_size": 1209800, /* byte */
///         "exchange_name": "bittrex",
///         "currency": "btc", // eth, usd, usdt, gbp, eur, cny, krw, jpy
///         "asset": "neo",
///         "asset_type": "options",
///         "first_epoch": 1512435389000, /* epoch begin in ms */
///         "last_epoch": 1512495389000, /* epoch begin in ms */
///         "total_updates": 1000000000, /* number of records in the file */
///         "assert_continuity": true, /* hopefully, the data is continuous */
///         "discontinuities": [ /* if the file has discontinuities */
///             {
///                 "begin": 1512495389000,
///                 "end": 1512495189000
///             },
///             {
///                 "begin": 1212495389000,
///                 "end": 1511495189000
///             }
///         ],
///         "uuid": "66b21989-291f-47c7-896e-02b1b1df8fc3", /* this is for future use */
///         "filename": "66b21989-291f-47c7-896e-02b1b1df8fc3", /* fname on gcloud */
///         "continuation_candles": false,

///         // chunk information, for redundancy
///         "chunked": false, /* is the data chunked? */
///         "chunk_parts": 3, /* if yes, how many chunks */
///         "x_of_n": 1, /* this is the first chunk of three */
///         "chunk_hash": "fdf3...", /* should be same as above */
///         "batch_hash": "fgdf3cfdb724f7ed282cb4f7e34349c05aaaa8125bc51daeca3456ce6646c3586",

///         // additional properties
///         // may store analytics results here
///         "tags": [
///             "futures",
///             "wash-trading"
///         ],
///         "errors": [], /* currently unused */
///         "_prefix": "" /* optional field for future emergency use */
///     }
/// }
/// 

use dtf::storage::DTFFileMetadata;
use dtf::file_metadata::FileMetadata;

use std::fmt;

enum GStorageFileOp {
    ADD_DTF,
}

impl fmt::Display for GStorageFileOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &GStorageFileOp::ADD_DTF => 
                write!(f, "add.dtf"),
        }
    }
}

pub struct GStorageOpMetadata {
    /*-------------- operation -------------*/
    op_type: GStorageFileOp,
    start_ts: u64,
    finish_ts: u64,
    response_time: u32,
    request_size: u32,
    req_hash: String,

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
            op_type: GStorageFileOp::ADD_DTF,

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

            ..Default::default()
        }
    }
}




//----------------------------------------------


pub struct GStorageMetadata<T: FileMetadata> {
    // meta section: about storage operation
    meta: GStorageOpMetadata,
    // data section: about the file itself
    data: T

}

impl<T: FileMetadata> GStorageMetadata<T> {
    pub fn new() -> GStorageMetadata<T> {
        GStorageMetadata {
            ..Default::default()
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

