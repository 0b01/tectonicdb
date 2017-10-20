/// Server should handle requests similar to Redis
/// 
/// List of commands:
/// -------------------------------------------

use byteorder::{BigEndian, WriteBytesExt, /*ReadBytesExt*/};

use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::str;

use state::*;
use utils;
use handler;
use settings::Settings;
use threadpool::ThreadPool;
use std::sync::{Arc, RwLock};


fn handle_client(mut stream: TcpStream, global: &Arc<RwLock<SharedState>>) {
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
        println!("Received:\t{:?}", req);

        let resp = handler::gen_response(&req, &mut state);
        match resp {
            (Some(str_resp), None, _) => {
                stream.write_u8(0x1).unwrap();
                stream.write_u64::<BigEndian>(str_resp.len() as u64).unwrap();
                stream.write(str_resp.as_bytes()).unwrap()
            },
            (None, Some(bytes), _) => {
                stream.write_u8(0x1).unwrap();
                stream.write(&bytes).unwrap()
            },
            (None, None, Some(errmsg)) => {
                stream.write_u8(0x0).unwrap();
                let ret = format!("ERR: {}\n", errmsg);
                stream.write_u64::<BigEndian>(ret.len() as u64).unwrap();
                stream.write(ret.as_bytes()).unwrap()
            },
            _ => panic!("IMPOSSIBLE")
        };
    }
}

pub fn run_server(host : &str, port : &str, settings: &Settings) {
    let addr = format!("{}:{}", host, port);

    if settings.verbosity > 1 {
        println!("[DEBUG] Trying to bind to addr: {}", addr);
        if settings.autoflush {
            println!("[DEBUG] Autoflush is true: every {} inserts.", settings.flush_interval);
        }
    }

    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => panic!(format!("{:?}", e.description()))
    };

    if settings.verbosity > 0 {
        println!("Listening on addr: {}", addr);
    }

    let pool = ThreadPool::new(settings.threads);
    let global = Arc::new(RwLock::new(SharedState::new(settings.clone()))); 

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

fn on_connect(global: &Arc<RwLock<SharedState>>) {
    {
        let mut glb_wtr = global.write().unwrap();
        glb_wtr.connections += 1;
    }

    let verbose = global.read().unwrap().settings.verbosity;
    if verbose > 0 {
        println!("Client connected. Current: {}.", global.read().unwrap().connections);
    } 
}

fn on_disconnect(global: &Arc<RwLock<SharedState>>) {
    {
        let mut glb_wtr = global.write().unwrap();
        glb_wtr.connections -= 1;
    }

    let rdr = global.read().unwrap();
    let verbose = rdr.settings.verbosity;
    if verbose > 0 {
        println!("Client connection disconnected. Current: {}.", rdr.connections);
    } 
}