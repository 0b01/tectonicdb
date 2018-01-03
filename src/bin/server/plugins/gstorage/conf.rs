use plugins::gstorage::config;
use std::collections::HashMap;
use std::default::Default;
use std::error;

#[cfg(test)]
static GSTORAGE_CONF_FNAME: &str = "conf/gstorage/example.conf.toml";
#[cfg(not(test))]
static GSTORAGE_CONF_FNAME: &str = "conf/gstorage/conf";

#[derive(Debug)]
pub struct GStorageConfig {
    /// configuration object
    pub conf: HashMap<String, String>,
    /// google cloud oauth token
    pub oauth_token: Option<String>,
    /// name of the bucket
    /// i.e. gs://tick-data
    pub bucket_name: String,
    /// folder name in bucket
    /// gs://tick-data/{folder}
    pub folder: String,
    /// upload interval in seconds
    pub interval: u64,
    /// remove file when it's done?
    pub remove: bool,
    /// data collection backend - if you don't know just ignore
    pub dcb: bool,
}

impl GStorageConfig {
    pub fn new() -> Result<GStorageConfig, Box<error::Error>> {
        let conf = GStorageConfig::get_conf();
        let oauth_token = {
            if conf.contains_key("oauth") {
                Some(conf.get("oauth").unwrap().to_owned())
            } else {
                None
            }
        };

        let bucket_name = conf.get("bucket-name").unwrap().to_owned();
        let folder = match conf.get("folder") {
            Some(&ref f) => f.to_owned(),
            None => "".to_owned(),
        };

        // upload interval
        let interval = match conf.get("interval") {
            Some(ref i) => i.parse()?,
            None => 3600,
        };

        let remove = match conf.get("remove") {
            Some(ref f) => f.to_owned() == "true",
            None => false,
        };

        let dcb = match conf.get("dcb") {
            Some(ref f) => f.to_owned() == "true",
            None => false,
        };


        Ok(GStorageConfig {
            conf,
            oauth_token,
            bucket_name,
            folder,
            interval,
            remove,
            dcb,
        })

    }

    fn get_conf() -> HashMap<String, String> {
        let mut settings = config::Config::default();
        settings
            .merge(config::File::with_name(GSTORAGE_CONF_FNAME))
            .unwrap();
        settings.deserialize::<HashMap<String, String>>().unwrap()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn should_return_config() {
//         let config = GStorageConfig::new();
//     }
// }
