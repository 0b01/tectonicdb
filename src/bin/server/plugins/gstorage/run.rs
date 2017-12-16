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

use plugins::gstorage::upload;

pub fn run(global: Arc<RwLock<SharedState>> ) {
    let global_copy = global.clone();
    let h = thread::spawn(move || {
        let conf = GStorageConfig::new();
        let interval = conf.interval;

        loop {
            // sleep for interval (default: 3600 secs = 1 hr)
            let dur = Duration::from_secs(interval);
            thread::sleep(dur);

            {
                let rdr = global_copy.read().unwrap();
                let folder = &rdr.settings.dtf_folder;

                for entry in fs::read_dir(folder).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();

                    if need_to_upload(&path, &dur) {
                        // upload
                        {
                            let fname = path.to_str().unwrap();
                            let meta = upload::upload(fname, &conf);
                            info!("{}", meta);
                        }

                        if conf.remove {
                            let _ = fs::remove_file(path);
                        }
                    }
                }
            }
        }
    });
}

fn need_to_upload(fname: &path::PathBuf, dur: &Duration) -> bool {
    fname.is_file()                         // if is file
    && is_dtf(fname.to_str().unwrap())      // dtf
    && time::SystemTime::now()
        .duration_since(fs::metadata(fname)
        .unwrap().modified().unwrap())
        .unwrap() <= *dur                   // file modified after
}