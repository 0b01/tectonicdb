/// The uploader is run in a separate thread spawned from server thread,
///
/// The thread sleeps until next midnight,
/// then upload all the dtf files to google storage via REST endpoint
/// and once confirmed, delete local files.

use std::{thread, time, fs, path, io, error};
use std::time::Duration;
use std::sync::{Arc, RwLock};
use state::SharedState;
use plugins::gstorage::GStorageConfig;
use libtectonic::dtf::is_dtf;

use plugins::gstorage::upload::{self, GStorageFile};

fn get_files_to_upload(entries: fs::ReadDir, dur: Duration) -> Vec<io::Result<fs::DirEntry>> {
    entries
        .into_iter()
        .filter(|entry| {
            let entry = match entry {
                &Ok(ref e) => e,
                &Err(ref e) => {
                    error!("Unable to get Dir Entry: {:?}", e);
                    return false;
                }
            };
            let path = entry.path();

            needs_to_upload(&path, &dur).unwrap_or_else(|e| {
                error!("Cannot determine whether to upload. e: {:?}", e);
                false
            })
        })
        .collect::<Vec<_>>()
}

fn upload_file(entry_res: io::Result<fs::DirEntry>, conf: &GStorageConfig) {
    let entry = match entry_res {
        Ok(entry) => entry,
        Err(err) => {
            error!("Error while attempting to upload directory entry: {:?}", err);
            return;
        },
    };
    let path = entry.path();

    {
        let fname = match path.to_str() {
            Some(p) => p,
            None => {
                error!("Unable to convert filename");
                return;
            }
        };

        let mut f = match GStorageFile::new(&conf, fname) {
            Ok(f) => f,
            Err(e) => {
                error!("fname: {}, {:?}", fname, e);
                return;
            },
        };

        match upload::upload(&mut f, fname) {
            Ok(metadata) => {
                if let Some(ref dcb_url) = conf.dcb_url {
                    match upload::post_to_dcb(&dcb_url, &metadata) {
                        Ok(res) => info!("Response from DCB: {:?}", res),
                        Err(err) => error!("Error while posting data to DCB: {:?}", err),
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
                    let files_to_upload = get_files_to_upload(entries, dur);

                    let count = files_to_upload.len();
                    info!("Need to upload {} files.", count);

                    for entry in files_to_upload {
                        upload_file(entry, &conf);
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
