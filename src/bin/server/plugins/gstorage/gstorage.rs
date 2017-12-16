/// upload saved dtf file to google cloud storage

use plugins::gstorage::reqwest;
use plugins::gstorage::reqwest::Body;
use std::io::Read;

use plugins::gstorage::conf::GStorageConfig;
use plugins::gstorage::metadata::GStorageOpMetadata;

use dtf::file_metadata;
use plugins::gstorage::GStorageMetadata;
use plugins::gstorage::serde_json;

use std::path::Path;

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

    pub fn new(conf: GStorageConfig, fname: &str) -> GStorageFile {

        let name = Path::new(fname).file_name().unwrap();

        let remote_name = format!("{}-{}", Uuid::new_v4(), name.to_str().unwrap());

        GStorageFile {
            fname: fname.to_owned(),
            remote_name,
            bucket_name: conf.bucket_name,
            folder: conf.folder,
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
        let start_ts = time::now();

        let uri = format!(
            "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}/{}",
                self.bucket_name,
                self.folder,
                self.remote_name);

        let body = self.file_content();

        let client = reqwest::Client::new();
        let mut res = client.post(&uri)
            .body(body)
            .send()
            .unwrap();


        if res.status().is_success() {
            let mut content = String::new();
            let _ = res.read_to_string(&mut content);

            // get end time
            let end_ts = time::now();

            self.uploaded = true;
            return Some(self.parse_resp(content, start_ts.to_timespec().sec as u32, end_ts.to_timespec().sec as u32));
        } else {
            // TODO: smooth failure
            panic!("Upload failed!");
            // return None;
        }

    }

    fn parse_resp(&self, resp: String, start_ts: u32, end_ts: u32) -> GStorageOpMetadata {
        let mut meta = GStorageOpMetadata::new(resp);
        meta.start_ts = start_ts;
        meta.finish_ts = end_ts;
        println!("{:?} {:?}", end_ts, start_ts);
        meta.response_time = end_ts - start_ts;

        meta
    }
}

pub fn process_file(fname: &str) -> String {
    let conf = GStorageConfig::new();

    let mut f = GStorageFile::new(conf, fname);
    let op_meta = f.upload().unwrap();
    let file_meta = file_metadata::from_fname(fname);
    let metadata = GStorageMetadata::new(op_meta, file_meta);
    let json = serde_json::to_string(&metadata).unwrap();
    json
}


#[cfg(test)]
mod tests {
    use super::*;
    use dtf::file_metadata;
    use plugins::gstorage::GStorageMetadata;
    use plugins::gstorage::serde_json;

    #[test]
    fn should_upload_file_to_gcloud() {

        let conf = GStorageConfig::new();
        let fname = "test-data/pl_btc_nav.dtf";
        let mut f = GStorageFile::new(conf, fname);
        let op_meta = f.upload().unwrap();
        let file_meta = file_metadata::from_fname(fname);

        let metadata = GStorageMetadata::new(op_meta, file_meta);

        let json = serde_json::to_string(&metadata).unwrap();

        println!("{}", json);
        
        println!("DONE");

    }
}