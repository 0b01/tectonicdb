/// Server should handle requests similar to Redis
/// 
/// List of commands:
/// -------------------------------------------

use byteorder::{WriteBytesExt, NetworkEndian, /*ReadBytesExt*/ };

use std::str;
use std::time;
use std::error::Error;
use std::io::{Read, Write};
// use std::net::TcpListener;
// use std::net::TcpStream;
use std::net::SocketAddr;
use std::io::{BufReader};

use state::*;
use handler::ReturnType;
use utils;
use handler;
use settings::Settings;
use threadpool::ThreadPool;
use std::sync::{Arc, RwLock};

use futures::prelude::*;
use futures::stream::Then;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;
use tokio_io::io::{lines, write_all, WriteHalf};

use plugins::run_plugins;

pub fn run_server(host : &str, port : &str, settings: &Settings) {
    let addr = format!("{}:{}", host, port);
    let addr = addr.parse::<SocketAddr>().unwrap();

    info!("Trying to bind to addr: {}", addr);
    if !settings.autoflush {
        warn!("Autoflush is off!");
    }
    debug!("Autoflush is {}: every {} inserts.", settings.autoflush, settings.flush_interval);
    debug!("Maximum connection: {}.", settings.threads);
    debug!("History granularity: {}.", settings.hist_granularity);

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let listener = TcpListener::bind(&addr, &handle).expect("failed to bind");


    let dtf_folder = &settings.dtf_folder;
    utils::create_dir_if_not_exist(&dtf_folder);

    info!("Listening on addr: {}", addr);
    info!("----------------- initialized -----------------");

    let pool = ThreadPool::new(settings.threads);
    let global = Arc::new(RwLock::new(SharedState::new(settings.clone()))); 


    run_plugins(global.clone());

    // main loop
    let done = listener.incoming().for_each(move |(socket, _addr)| {
        let global_copy = global.clone();
        let mut state = State::new(&global);
        utils::init_dbs(&mut state);
        on_connect(&global_copy);

        let (reader, writer) = socket.split();
        let lines = lines(BufReader::new(reader));
        let responses = lines.map(move |line| {
            handler::gen_response(&line, &mut state)
        });
        let writes = responses.fold(writer, |wtr, resp| {
            let mut buf: Vec<u8> = vec![];
            match resp {
                ReturnType::Bytes(bytes)  => {
                    buf.write_u8(0x1).unwrap();
                    buf.write(&bytes).unwrap();
                },
                ReturnType::String(str_resp) => {
                    buf.write_u8(0x1).unwrap();
                    buf.write_u64::<NetworkEndian>(str_resp.len() as u64).unwrap();
                },
                ReturnType::Error(errmsg) => {
                    // error!("Req: `{}`", line);
                    error!("Err: `{}`", errmsg.clone());
                    buf.write_u8(0x0).unwrap();
                    let ret = format!("ERR: {}\n", errmsg);
                    buf.write_u64::<NetworkEndian>(ret.len() as u64).unwrap();
                    buf.write(ret.as_bytes()).unwrap();
                }
            };
            write_all(wtr, buf).map(|(w,_)| w)
        });

        // let writes = responses.then(move |_| Ok(()));


        let msg = writes.then(move |_| Ok(()));
        handle.spawn(msg);

        on_disconnect(&global_copy);

        Ok(())
    });

    core.run(done).unwrap();
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