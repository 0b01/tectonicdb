/// Server should handle requests similar to Redis
///
/// List of commands:
/// -------------------------------------------

use byteorder::{WriteBytesExt, NetworkEndian /*ReadBytesExt*/};

use std::str;
use std::io::Write;
use std::net::SocketAddr;
use std::io::BufReader;
use std::collections::HashMap;

use std::borrow::{Borrow, Cow};
use state::{Global, SharedState, ThreadState};
use handler::ReturnType;
use utils;
use handler;
use settings::Settings;
use std::sync::{Arc, RwLock};

use futures::prelude::*;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;
use tokio_io::io::{lines, write_all};

use plugins::run_plugins;

pub fn run_server(host: &str, port: &str, settings: &Settings) {
    let addr = format!("{}:{}", host, port);
    let addr = addr.parse::<SocketAddr>().unwrap();

    info!("Trying to bind to addr: {}", addr);
    if !settings.autoflush {
        warn!("Autoflush is off!");
    }
    info!(
        "Autoflush is {}: every {} inserts.",
        settings.autoflush,
        settings.flush_interval
    );
    info!("History granularity: {}.", settings.hist_granularity);

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let listener = TcpListener::bind(&addr, &handle).expect("failed to bind");


    let dtf_folder = &settings.dtf_folder;
    utils::create_dir_if_not_exist(&dtf_folder);

    info!("Listening on addr: {}", addr);
    info!("----------------- initialized -----------------");

    let global = Arc::new(RwLock::new(SharedState::new(settings.clone())));
    let store = Arc::new(RwLock::new(HashMap::new()));

    run_plugins(global.clone());

    use std::rc::Rc;
    use std::cell::RefCell;
    // main loop
    let done = listener.incoming().for_each(move |(socket, _addr)| {
        let global_copy = global.clone();
        let state = Rc::new(RefCell::new(ThreadState::new(global.clone(), store.clone())));
        let state_clone = state.clone();

        match utils::init_dbs(&mut state.borrow_mut()) {
            Ok(()) => (),
            Err(_) => panic!("Cannot initialized db!"),
        };
        on_connect(&global_copy);

        let (rdr, wtr) = socket.split();
        let lines = lines(BufReader::new(rdr));
        let responses = lines.map(move |line| {
            let line: Cow<str> = line.into();
            let resp = handler::gen_response(line.borrow(), &mut state.borrow_mut());
            (line, resp)
        });
        let writes = responses.fold(wtr, |wtr, (line, resp)| {
            let mut buf: Vec<u8> = vec![];
            use self::ReturnType::*;
            match resp {
                Bytes(bytes) => {
                    buf.write_u8(0x1).unwrap();
                    buf.write_u64::<NetworkEndian>(bytes.len() as u64)
                        .unwrap();
                    buf.write(&bytes).unwrap();
                }
                String(str_resp) => {
                    buf.write_u8(0x1).unwrap();
                    buf.write_u64::<NetworkEndian>(str_resp.len() as u64)
                        .unwrap();
                    buf.write(str_resp.as_bytes()).unwrap();
                }
                Error(errmsg) => {
                    error!("Req: `{}`", line);
                    error!("Err: `{}`", errmsg.clone());
                    buf.write_u8(0x0).unwrap();
                    let ret = format!("ERR: {}\n", errmsg);
                    buf.write_u64::<NetworkEndian>(ret.len() as u64).unwrap();
                    buf.write(ret.as_bytes()).unwrap();
                }
            };
            write_all(wtr, buf).map(|(w, _)| w)
        });

        let msg = writes.then(move |_| {
            state_clone.borrow_mut().unsub();
            on_disconnect(&global_copy);
            Ok(())
        });
        handle.spawn(msg);

        Ok(())
    });

    core.run(done).unwrap();
}

fn on_connect(global: &Global) {
    {
        let mut glb_wtr = global.write().unwrap();
        glb_wtr.n_cxns += 1;
    }

    info!(
        "Client connected. Current: {}.",
        global.read().unwrap().n_cxns
    );
}

fn on_disconnect(global: &Global) {
    {
        let mut glb_wtr = global.write().unwrap();
        glb_wtr.n_cxns -= 1;
    }

    let rdr = global.read().unwrap();
    info!("Client connection disconnected. Current: {}.", rdr.n_cxns);
}
