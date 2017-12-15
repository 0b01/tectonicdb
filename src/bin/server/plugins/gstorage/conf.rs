extern crate serde;
extern crate serde_json;

use self::serde_json as json;
use std::fs::File;
use std::io::{Read};

extern crate config;
use std::collections::HashMap;
use std::default::Default;

pub struct GStorageConfig {
    pub conf: HashMap<String, String>,
    pub oauth_token: Option<String>,
    pub bucket_name: String,
}

impl GStorageConfig {

    pub fn new() -> GStorageConfig {
        let conf = GStorageConfig::get_conf();
        let oauth_token = {
            if conf.contains_key("oauth") {
                Some(conf
                        .get("oauth")
                        .unwrap()
                        .to_owned())
            } else {
                None
            }
        };

        let bucket_name = conf
                            .get("bucket-name")
                            .unwrap()
                            .to_owned();

        GStorageConfig {
            conf,
            oauth_token,
            bucket_name
        }
    }

    fn get_conf() -> HashMap<String, String> {
        let fname = "conf/gstorage/conf";
        let mut settings = config::Config::default();
        settings.merge(config::File::with_name(fname)).unwrap();
        settings.deserialize::<HashMap<String, String>>().unwrap()
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn should_return_config() {
        let config = GStorageConfig::new();
    }
}