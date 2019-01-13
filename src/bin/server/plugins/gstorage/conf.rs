use std::error::Error;

use crate::settings::{key_or_default_parse, key_or_default, key_or_none};

#[derive(Debug)]
pub struct GStorageConfig {
    /// google cloud oauth token
    pub oauth_token: Option<String>,
    /// name of the bucket
    /// i.e. gs://tick-data
    pub bucket_name: String,
    /// folder name in bucket
    /// gs://tick-data/{folder}
    pub folder: String,
    /// remove file when it's done?
    pub remove: bool,
    /// data collection backend - if you don't know just ignore
    pub dcb_url: Option<String>,
    /// amount of seconds between upload checks
    pub upload_interval_secs: u64,
    /// min file size that will be uploaded to google cloud storage
    pub min_file_size: u64,
}

impl GStorageConfig {
    /// Creates a new `GStorageConfig` from environment variables, filling in the remaining values with
    /// defaults if not available.
    pub fn new() -> Result<Self, Box<Error>> {
        Ok(GStorageConfig {
            oauth_token: key_or_none("GCLOUD_OAUTH_TOKEN"),
            bucket_name: key_or_default("GCLOUD_BUCKET_NAME", "tick_data"),
            folder: key_or_default("GCLOUD_FOLDER", ""),
            remove: key_or_default_parse("GCLOUD_REMOVE_ON_UPLOAD", true)?,
            dcb_url: key_or_none("DCB_URL"),
            upload_interval_secs: key_or_default_parse("GCLOUD_UPLOAD_INTERVAL_SECS", 30)?,
            min_file_size: key_or_default_parse("GCLOUD_MIN_FILE_SIZE_BYTES", 1024 * 1024)?,
        })
    }
}
