/// upload saved dtf file to google cloud storage
use plugins::gstorage::serde;
use plugins::gstorage::serde_json;

use plugins::gstorage::reqwest;
use plugins::gstorage::reqwest::Body;
use std::io::Read;

use plugins::gstorage::conf::GStorageConfig;
use plugins::gstorage::metadata::{ GStorageMetadata, GStorageOpMetadata };
use dtf::storage::file_metadata::FileMetadata;
use dtf::storage::file_metadata;

use std::fs::File;
extern crate time;

#[derive(Debug)]
pub struct GStorageFile {
    fname: String,
    bucket_name: String,
    uploaded: bool,
}

impl GStorageFile {

    pub fn new(fname: &str) -> GStorageFile {

        let conf = GStorageConfig::new();

        GStorageFile {
            fname: fname.to_owned(),
            bucket_name: conf.bucket_name,
            uploaded: false,
        }

    }

    fn file_content(&self) -> Body {
        let file = File::open(&self.fname).unwrap();
        let body = Body::new(file);
        body
    }

    fn upload(&mut self) -> Option<GStorageOpMetadata> {

        // get start time
        let _timespec = time::get_time(); let start_ts = _timespec.sec + _timespec.nsec as i64 / 1000 / 1000;

        let uri = format!(
            "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
                self.bucket_name,
                self.fname);

        let body = self.file_content();

        let client = reqwest::Client::new();
        let mut res = client.post(&uri)
            .body(body)
            .send()
            .unwrap();

        // get end time
        let _timespec = time::get_time(); let end_ts = _timespec.sec + _timespec.nsec as i64 / 1000 / 1000;

        if res.status().is_success() {
            let mut content = String::new();
            res.read_to_string(&mut content);

            self.uploaded = true;
            return Some(self.parse_resp(content, start_ts as u32, end_ts as u32));
        } else {
            // TODO: smooth failure
            panic!("Upload failed!");
            return None;
        }
    }

    fn parse_resp(&self, resp: String, start_ts: u32, end_ts: u32) -> GStorageOpMetadata {
        let mut meta = GStorageOpMetadata::new(resp);
        meta.start_ts = start_ts;
        meta.finish_ts = end_ts;
        meta.response_time = end_ts - start_ts;

        meta
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_upload_file_to_gcloud() {

        let fname = "test-data/pl_btc_nav.dtf";
        let mut f = GStorageFile::new(fname);
        let op_meta = f.upload().unwrap();
        let file_meta = file_metadata::from_fname(fname);

        let metadata = GStorageMetadata::new(op_meta, file_meta);

        let json = serde_json::to_string(&metadata).unwrap();

        println!("{}", json);
        
        println!("DONE");
    }
}