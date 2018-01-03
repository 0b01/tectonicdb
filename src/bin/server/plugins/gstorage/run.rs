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
use libtectonic::dtf::is_dtf;
use std::error;
use std::io;

use plugins::gstorage::upload;

pub fn run(global: Arc<RwLock<SharedState>>) {
    let global_copy = global.clone();
    thread::spawn(move || {
        let conf = GStorageConfig::new().unwrap();
        info!("{:?}", conf);
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
                    let entries = entries
                        .filter(|entry| {
                            let entry = match entry {
                                &Ok(ref e) => e,
                                &Err(ref e) => {
                                    error!("Unable to get Dir Entry: {:?}", e);
                                    return false;
                                }
                            };
                            let path = entry.path();

                            return needs_to_upload(&path, &dur).unwrap_or_else(|e| {
                                error!("Cannot determine whether to upload. e: {:?}", e);
                                false
                            });
                        })
                        .collect::<Vec<_>>();


                    let count = entries.len();
                    info!("Need to upload {} files.", count);

                    for entry in entries {
                        // upload
                        let entry = entry.unwrap();
                        let path = entry.path();

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
                                Ok(m) => {
                                    info!("GS: {}", fname);

                                    if conf.dcb {
                                        match upload::post_to_dcb(&m) {
                                            Ok(_) => info!("DCB: {}", fname),
                                            Err(_) => error!("Error DCB: {}", fname),
                                        }
                                    }

                                }
                                Err(e) => error!("fname: {}, {:?}", fname, e),
                            };
                        }

                        if conf.remove {
                            let _ = fs::remove_file(path);
                        }
                    }
                }
            };
        }
    });
}

fn needs_to_upload(fname: &path::PathBuf, dur: &Duration) -> Result<bool, Box<error::Error>> {
    let fname_str = match fname.to_str() {
        Some(n) => n,
        None => {
            return Err(box io::Error::new(
                io::ErrorKind::InvalidInput,
                "fname is not valid unicode",
            ))
        }
    };

    Ok(
        fname.is_file()                                     // if is file
        && is_dtf(fname_str).unwrap()                       // dtf
        && time::SystemTime::now().duration_since(
                fs::metadata(fname)?.modified()?)? <= *dur, // file modified after
    )
}
