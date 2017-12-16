/// upload saved dtf file to google cloud storage
extern crate reqwest;
extern crate serde;
extern crate serde_json;

use self::reqwest::Body;
use std::io::Read;

use plugins::gstorage::conf::GStorageConfig;
use plugins::gstorage::metadata::{ GStorageMetadata, GStorageOpMetadata };
use dtf::storage::file_metadata::FileMetadata;
use dtf::storage::file_metadata;

use std::fs::File;

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

        if res.status().is_success() {
            let mut content = String::new();
            res.read_to_string(&mut content);

            self.uploaded = true;

            println!("{:?}", content);

            let json = serde_json::from_str::<serde_json::Value>(&content).unwrap();
            return Some(self.parse_resp(json));
        } else {
            unimplemented!();
            return None;
        }
    }

    fn parse_resp(&self, resp: serde_json::Value) -> GStorageOpMetadata {
        unimplemented!()
    }

    fn to_metadata<T>(op_meta: GStorageOpMetadata, file_meta: T) -> GStorageMetadata<T> 
        where T: FileMetadata
    {
        unimplemented!()
    }

}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_upload_file_to_gcloud() {

        let fname = "test-data/pl_btc_nav.dtf";
        let mut f = GStorageFile::new(fname);
        let op_meta = f.upload();

        let file_meta = file_metadata::from_fname(fname);
        
        println!("DONE");
    }
}