/// Server should handle requests similar to Redis
/// 
/// List of commands:
/// -------------------------------------------

use byteorder::{WriteBytesExt, NetworkEndian, /*ReadBytesExt*/ };

use std::{str, thread, time};
use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;

use state::*;
use handler::ReturnType;
use utils;
use handler;
use settings::Settings;
use threadpool::ThreadPool;
use std::sync::{Arc, RwLock};

fn respond(mut stream: &TcpStream, mut state: &mut State, line: &str) {
    let resp = handler::gen_response(&line, &mut state);
    match resp {
        ReturnType::Bytes(bytes)  => {
            stream.write_u8(0x1).unwrap();
            stream.write(&bytes).unwrap()
        }
        ReturnType::String(str_resp) => {
            stream.write_u8(0x1).unwrap();
            stream.write_u64::<NetworkEndian>(str_resp.len() as u64).unwrap();
            stream.write(str_resp.as_bytes()).unwrap()
        },
        ReturnType::Error(errmsg) => {
            error!("Req: `{}`", line);
            error!("Err: `{}`", errmsg.clone());

            stream.write_u8(0x0).unwrap();
            let ret = format!("ERR: {}\n", errmsg);
            stream.write_u64::<NetworkEndian>(ret.len() as u64).unwrap();
            stream.write(ret.as_bytes()).unwrap()
        }
    };
}

fn handle_client(mut stream: TcpStream, global: &LockedGlobal) {
    let settings = {
        let shared_state = global.read().unwrap();
        &shared_state.settings.clone()
    };
    let dtf_folder = &settings.dtf_folder;
    utils::create_dir_if_not_exist(&dtf_folder);

    let mut state = State::new(global);
    utils::init_dbs(&mut state);

    let mut buf = [0; 2048];
    loop {
        let bytes_read = stream.read(&mut buf).unwrap();
        if bytes_read == 0 { break }
        let req = str::from_utf8(&buf[..(bytes_read-1)]).unwrap();
        for line in req.split('\n') {
            // println!("[DEBUG] Received:\t{:?}", line);
            respond(&stream, &mut state, &line);
        }
    }
}

pub fn run_server(host : &str, port : &str, settings: &Settings) {
    let addr = format!("{}:{}", host, port);

    info!("Trying to bind to addr: {}", addr);
    if !settings.autoflush {
        warn!("Autoflush is off!");
    }
    debug!("Autoflush is {}: every {} inserts.", settings.autoflush, settings.flush_interval);
    debug!("Maximum connection: {}.", settings.threads);
    debug!("History granularity: {}.", settings.hist_granularity);

    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => panic!(format!("{:?}", e.description()))
    };

    info!("Listening on addr: {}", addr);
    info!("-----------------initiated-----------------");

    let pool = ThreadPool::new(settings.threads);
    let global = Arc::new(RwLock::new(SharedState::new(settings.clone()))); 

    // Timer for recording history
    {
        let global_copy_timer = global.clone();
        let granularity = settings.hist_granularity.clone();
        thread::spawn(move || {
            let dur = time::Duration::from_secs(granularity);
            loop {
                {
                    let mut rwdr = global_copy_timer.write().unwrap();
                    let (total, sizes) = {
                        let mut total = 0;
                        let mut sizes: Vec<(String, u64)> = Vec::new();
                        for (name, vec) in rwdr.vec_store.iter() {
                            let size = vec.1;
                            total += size;
                            sizes.push((name.clone(), size));
                        }
                        sizes.push(("total".to_owned(), total));
                        (total, sizes)
                    };

                    let current_t = time::SystemTime::now();
                    for &(ref name, size) in sizes.iter() {
                        if !rwdr.history.contains_key(name) {
                            rwdr.history.insert(name.clone(), Vec::new());
                        }
                        rwdr.history.get_mut(name)
                                    .unwrap()
                                    .push((current_t, size));
                    }

                    info!("Current total count: {}", total);
                }

                thread::sleep(dur);
            }
        });
    }

    // main loop
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let global_copy = global.clone();
        pool.execute(move || {
            on_connect(&global_copy);
            handle_client(stream, &global_copy);
            on_disconnect(&global_copy);
        });
    }
}

type LockedGlobal = Arc<RwLock<SharedState>>;

fn on_connect(global: &LockedGlobal) {
    {
        let mut glb_wtr = global.write().unwrap();
        glb_wtr.n_cxns += 1;
    }

    info!("Client connected. Current: {}.", global.read().unwrap().n_cxns);
}

fn on_disconnect(global: &LockedGlobal) {
    {
        let mut glb_wtr = global.write().unwrap();
        glb_wtr.n_cxns -= 1;
    }

    let rdr = global.read().unwrap();
    info!("Client connection disconnected. Current: {}.", rdr.n_cxns);
}