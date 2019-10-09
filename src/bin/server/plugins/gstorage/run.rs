/// The uploader is run in a separate thread spawned from server thread,
///
/// The thread sleeps until next midnight,
/// then upload all the dtf files to google storage via REST endpoint
/// and once confirmed, delete local files.

use std::{thread, fs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

extern crate tempdir;
use self::tempdir::TempDir;

use crate::plugins::gstorage::GStorageConfig;
use crate::plugins::gstorage::upload::{self, GStorageFile};

/// Posts a DTF file's metadata to the DCB, uploads it to Google Cloud Storage, and then
/// optionally deletes it after.
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
            debug!("DTF file {} successfully uploaded to google cloud storage.", fname);
            if let Some(ref dcb_url) = conf.dcb_url {
                match upload::post_to_dcb(&dcb_url, &metadata) {
                    Ok(res) => info!("DTF file metadata posted to the DCB: {:?}", res),
                    Err(err) => error!("Error while posting data to DCB: {:?}", err),
                }
            }

        }
        Err(e) => error!("fname: {}, {:?}", fname, e),
    };

    if conf.remove {
        match fs::remove_file(path_buf.as_path()) {
            Ok(_) => debug!("DTF file successfully deleted."),
            Err(err) => error!("Error while deleting DTF file: {:?}", err),
        }
    }
}

fn upload_all_files(dir_path: &Path) {
    let conf = GStorageConfig::new().unwrap();

    // Upload all files in the directory
    for path_res in fs::read_dir(dir_path).unwrap() {
        match path_res {
            Ok(entry) => {
                // Upload the DTF file to Google Cloud Storage and post its metadata to
                // the DCB
                let file_path = entry.path();
                info!("Found file to upload: {:?}", file_path);
                upload_file(file_path, &conf);
            },
            Err(err) => error!("Error while reading dir entry: {:?}", err),
        }
    }
}

lazy_static! {
    static ref TMP_DIR: TempDir = tempdir::TempDir::new("tectonic")
        .expect("Unable to create temporary directory!");
}

pub fn run(global: Arc<RwLock<SharedState>>) {
    let global_copy = global.clone();
    thread::spawn(move || {
        let conf = GStorageConfig::new().unwrap();
        let min_file_size_bytes = conf.min_file_size;
        info!("Initializing GStorage plugin with config: {:?}", conf);
        let dtf_directory = global_copy.read().unwrap().settings.dtf_folder.clone();
        let tmp_dir_path = TMP_DIR.path();

        loop {
            thread::sleep(Duration::from_secs(conf.upload_interval_secs));
            info!("Gstorage checking to see if any files need upload...");

            // Move all DTF files in the db directory to the temporary directory for uploading
            for path_res in fs::read_dir(&dtf_directory).unwrap() {
                match path_res {
                    Ok(entry) => {
                        let src_path = entry.path();
                        let dtf_file_name = src_path.file_name()
                            .unwrap()
                            .to_str()
                            .unwrap();
                        let metadata = match entry.metadata() {
                            Ok(metadata) => metadata,
                            Err(err) => {
                                error!("Error while fetching DTF metadata: {:?}", err);
                                return;
                            },
                        };
                        let file_size_bytes: u64 = metadata.len();

                        if file_size_bytes >= min_file_size_bytes {
                            // move the file to the temporary directory to be uploaded
                            let dst_path = tmp_dir_path.join(dtf_file_name);
                            match fs::rename(src_path.clone(), dst_path) {
                                Ok(_) => (),
                                Err(err) => error!(
                                    "Error while moving DTF file for upload: {:?}",
                                    err
                                ),
                            }
                        }
                    },
                    Err(err) => error!("Error while reading dir entry: {:?}", err),
                }
            }

            // Upload all files in the temporary directory
            upload_all_files(tmp_dir_path);
        }
    });
}

/// Called when the database is being shut down.  Upload all files, regardless of size.
pub fn run_exit_hook(state: &ThreadState<'static, 'static>) {
    let dtf_dir_path = &state.global.read().unwrap().settings.dtf_folder;
    upload_all_files(&Path::new(dtf_dir_path))
}
