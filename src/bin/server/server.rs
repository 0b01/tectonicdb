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

fn respond(mut wtr: WriteHalf<TcpStream>, mut state: &mut State, line: &str) {
    let resp = handler::gen_response(&line, &mut state);
    match resp {
        ReturnType::Bytes(bytes)  => {
            wtr.write_u8(0x1).unwrap();
            wtr.write(&bytes).unwrap()
        }
        ReturnType::String(str_resp) => {
            wtr.write_u8(0x1).unwrap();
            wtr.write_u64::<NetworkEndian>(str_resp.len() as u64).unwrap();
            wtr.write(str_resp.as_bytes()).unwrap()
        },
        ReturnType::Error(errmsg) => {
            error!("Req: `{}`", line);
            error!("Err: `{}`", errmsg.clone());

            wtr.write_u8(0x0).unwrap();
            let ret = format!("ERR: {}\n", errmsg);
            wtr.write_u64::<NetworkEndian>(ret.len() as u64).unwrap();
            wtr.write(ret.as_bytes()).unwrap()
        }
    };
}

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
        on_connect(&global_copy);

        // handle client
        let mut state = State::new(&global);
        utils::init_dbs(&mut state);

        let (reader, writer) = socket.split();
        let mainloop = loop {
            let lines = lines(BufReader::new(reader));

            let responses = lines.map(move |line| {
                respond(writer, &mut state, &line);
            });

            responses.then(move |_| Ok(()));
        };

        on_disconnect(&global_copy);

        handle.spawn(mainloop);
        Ok(())
    });

    core.run(done).unwrap();


    // // main loop
    // for stream in listener.incoming() {
    //     let stream = stream.unwrap();
    //     let global_copy = global.clone();
    //     pool.execute(move || {
    //         on_connect(&global_copy);
    //         handle_client(stream, &global_copy);
    //         on_disconnect(&global_copy);
    //     });
    // }
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