/// upload saved dtf file to google cloud storage

use plugins::gstorage::reqwest;
use plugins::gstorage::reqwest::Body;
use std::io::Read;

use plugins::gstorage::conf::GStorageConfig;
use plugins::gstorage::metadata::GStorageOpMetadata;

use libtectonic::storage::file_metadata;
use plugins::gstorage::GStorageMetadata;
use plugins::gstorage::serde_json;

use std::path::Path;
use std::io;
use std::error;

use std::fs::File;
extern crate time;
extern crate uuid;

use self::uuid::Uuid;

#[derive(Debug)]
pub struct GStorageFile {
    fname: String,
    remote_name: String,
    bucket_name: String,
    folder: String,
    uploaded: bool,
}

impl GStorageFile {
    pub fn new(conf: &GStorageConfig, fname: &str) -> Result<GStorageFile, io::Error> {

        let name = Path::new(fname)
            .file_name()
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                "don't know filename",
            ))?
            .to_str()
            .ok_or(io::Error::new(
                io::ErrorKind::NotFound,
                "not a valid filename",
            ))?;

        let remote_name = format!("{}-{}", Uuid::new_v4(), name);

        Ok(GStorageFile {
            fname: fname.to_owned(),
            remote_name,
            bucket_name: conf.bucket_name.clone(),
            folder: conf.folder.clone(),
            uploaded: false,
        })

    }

    fn file_content(&self) -> Result<Body, io::Error> {
        let file = File::open(&self.fname)?;
        let body = Body::new(file);
        Ok(body)
    }

    pub fn upload(&mut self) -> Result<GStorageOpMetadata, Box<error::Error>> {

        // get start time
        let start_ts = time::now();

        let uri = format!(
            "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}/{}",
            self.bucket_name,
            self.folder,
            self.remote_name
        );

        let body = self.file_content();

        let client = reqwest::Client::new();
        let mut res = client.post(&uri).body(body?).send()?;


        if res.status().is_success() {
            let mut content = String::new();
            let _ = res.read_to_string(&mut content);

            // get end time
            let finish_ts = time::now();

            self.uploaded = true;

            Ok(GStorageOpMetadata::new(
                content,
                start_ts.to_timespec().sec as u32,
                finish_ts.to_timespec().sec as u32,
            )?)

        } else {
            Err(box io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Cannot upload file {}! dbg: {:?}",
                    self.fname,
                    res
                ),
            ))
        }

    }
}

pub fn upload(fname: &str, conf: &GStorageConfig) -> Result<String, Box<error::Error>> {
    let mut f = GStorageFile::new(conf, fname)?;
    let op_meta = f.upload()?;
    let file_meta = file_metadata::from_fname(fname)?;
    let metadata = GStorageMetadata::new(op_meta, file_meta);
    let json = serde_json::to_string(&metadata)?;
    Ok(json)
}

/// data collection backend is a proprietary data ingestion engine
pub fn post_to_dcb(url: &str, json: &str) -> Result<String, Box<error::Error>> {
    let client = reqwest::Client::new();
    let mut res = client
        .post(url)
        .body(json.to_owned())
        .send()?;
    Ok(res.text()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use libtectonic::storage::file_metadata;
    use plugins::gstorage::GStorageMetadata;
    use plugins::gstorage::serde_json;

    #[test]
    fn should_upload_file_to_gcloud() {
        let conf = GStorageConfig::new().unwrap();
        let fname = "test/test-data/pl_btc_nav.dtf";
        let mut f = GStorageFile::new(&conf, fname).unwrap();
        let op_meta = f.upload().unwrap();
        let file_meta = file_metadata::from_fname(fname).unwrap();

        let metadata = GStorageMetadata::new(op_meta, file_meta);
        let json = serde_json::to_string(&metadata).unwrap();

        println!("{}", json);
        if let Some(ref dcb_url) = conf.dcb_url {
            let res = post_to_dcb(&json, dcb_url).unwrap();
            println!("{}", res);
        }

        println!("DONE");
    }
}
