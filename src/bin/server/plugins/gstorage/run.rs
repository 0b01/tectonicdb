/// The uploader is run in a separate thread spawned from server thread,
/// 
/// The thread sleeps until next midnight,
/// then upload all the dtf files to google storage via REST endpoint
/// and once confirmed, delete local files.

use std::{thread, time, fs, path};
use std::time::Duration;
use std::sync::{Arc, RwLock};
use state::SharedState;
use plugins::gstorage::GStorageConfig;
use dtf::is_dtf;
use std::error;
use std::io;

use plugins::gstorage::upload;

pub fn run(global: Arc<RwLock<SharedState>> ) {
    let global_copy = global.clone();
    thread::spawn(move || {
        let conf = GStorageConfig::new().unwrap();
        let interval = conf.interval;

        let folder = {
            let rdr = global_copy.read().unwrap();
            &rdr.settings.dtf_folder.clone()
        };

        loop {
            // sleep for interval (default: 3600 secs = 1 hr)
            let dur = Duration::from_secs(interval);
            thread::sleep(dur);
            match fs::read_dir(folder) {
                Err(e) => error!("Unable to read dir entries: {:?}", e),
                Ok(entries) => {
                    for entry in entries {
                        let entry = match entry {
                            Ok(e) => e,
                            Err(e) => {
                                error!("Unable to get Dir Entry");
                                continue;
                            }
                        };
                        let path = entry.path();

                        let upload_file = needs_to_upload(&path, &dur).unwrap_or_else(|e| {
                                error!("Cannot determine whether to upload. e: {:?}", e);
                                false
                        });

                        if upload_file {
                            // upload
                            {
                                let fname = match path.to_str() {
                                    Some(p) => p,
                                    None => {
                                        error!("Unable to convert filename");
                                        continue;
                                    }
                                };

                                let meta = upload::upload(fname, &conf);
                                match meta {
                                    Ok(m) => info!("{}", m),
                                    Err(e) => error!("fname: {}, {:?}", fname, e)
                                };
                            }

                            if conf.remove {
                                let _ = fs::remove_file(path);
                            }
                        }
                    }
                },
            };
        }
    });
}

fn needs_to_upload(fname: &path::PathBuf, dur: &Duration) -> Result<bool, Box<error::Error>> {
    let fname_str = match fname.to_str() {
        Some(n) => n,
        None => return Err(
            box io::Error::new(
                io::ErrorKind::InvalidInput,
                "fname is not valid unicode"
            ))
    };

    Ok(
        fname.is_file()                                     // if is file
        && is_dtf(fname_str)                                // dtf
        && time::SystemTime::now().duration_since(
                fs::metadata(fname)?.modified()?)? <= *dur  // file modified after
    )
}