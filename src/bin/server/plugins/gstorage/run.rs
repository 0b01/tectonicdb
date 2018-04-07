/// The uploader is run in a separate thread spawned from server thread,
///
/// The thread sleeps until next midnight,
/// then upload all the dtf files to google storage via REST endpoint
/// and once confirmed, delete local files.

use std::{thread, time, fs, path, io, error};
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::time::Duration;

extern crate notify;
use self::notify::{RecommendedWatcher, Watcher, RecursiveMode, DebouncedEvent};

use state::SharedState;
use plugins::gstorage::GStorageConfig;
use plugins::gstorage::upload::{self, GStorageFile};
use libtectonic::dtf::is_dtf;

fn upload_file(path_buf: PathBuf, conf: &GStorageConfig) {
    let fname = match path_buf.to_str() {
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

    if conf.remove {
        let _ = fs::remove_file(path_buf.as_path());
    }
}

fn watch_directory(directory: &str, conf: &GStorageConfig) -> notify::Result<()> {
    let (tx, rx) = channel();

    let mut watcher: RecommendedWatcher = Watcher::new(tx, Duration::from_secs(2))?;
    watcher.watch(directory, RecursiveMode::Recursive)?;

    loop {
        let evt = rx.recv();
        println!("Event: {:#?}", evt);
        // match rx.recv() {
        //     Ok(evt) => match evt {
        //         DebouncedEvent::Create(path_buf) => upload_file(path_buf, &conf),
        //         DebouncedEvent::Write(path_buf) => upload_file(path_buf, &conf),
        //     },
        //     Err(err) => error!("Watch error: {:#?}", err),
        // }
    }
}

pub fn run(global: Arc<RwLock<SharedState>>) {
    let global_copy = global.clone();
    thread::spawn(move || {
        let conf = GStorageConfig::new().unwrap();
        info!("Initializing GStorage plugin with config: {:?}", conf);
        let dtf_directory = global_copy.read().unwrap().settings.dtf_folder.clone();

        watch_directory(&dtf_directory, &conf);
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
