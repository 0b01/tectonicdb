/// Server should handle requests similar to Redis
///
/// List of commands:
/// -------------------------------------------

use byteorder::{WriteBytesExt, NetworkEndian};

use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{BufReader, Write};
use std::net::SocketAddr;
use std::rc::Rc;
use std::str;
use std::sync::{Arc, RwLock};
use std::process::exit;

use state::{Global, SharedState, ThreadState, HashMapStore};
use handler::ReturnType;
use libtectonic::dtf::{Update, UpdateVecInto};
use utils;
use handler;
use plugins::run_plugins;
use settings::Settings;

use futures::prelude::*;
use futures::sync::mpsc;
use tokio_core::net::TcpListener;
use tokio_core::reactor::{Core, Handle};
use tokio_io::AsyncRead;
use tokio_io::io::{lines, write_all};
use tokio_signal;

#[cfg(unix)]
fn enable_platform_hook<'a>(
    handle: &Handle, 
    global: Global,
    store: HashMapStore<'a>) {
    let (subscriptions_tx, _) = mpsc::unbounded::<Update>();
    let signal_handler_threadstate = ThreadState::new(
        Arc::clone(&global),
        Arc::clone(&store),
        subscriptions_tx
    );

    /// Creates a listener for Unix signals that takes care of flushing all stores to file before
    /// shutting down the server.
    // Catches `TERM` signals, which are sent by Kubernetes during graceful shutdown.
    let signal_handler = tokio_signal::unix::Signal::new(15)
        .flatten_stream()
        .for_each(move |signal| {
            println!("Signal: {}", signal);
            info!("`TERM` signal recieved; flushing all stores...");
            signal_handler_threadstate.flushall();
            info!("All stores flushed; exiting...");
            exit(0);

            #[allow(unreachable_code)]
            Ok(())
        })
        .map_err(|err| error!("Error in signal handler future: {:?}", err));

    handle.spawn(signal_handler);
}

#[cfg(windows)]
fn enable_platform_hook<'a>(
    handle: &Handle, 
    global: Global,
    store: HashMapStore<'a>) {

}

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

    enable_platform_hook(&handle, Arc::clone(&global), Arc::clone(&store));
    
    run_plugins(global.clone());

    // main loop
    let done = listener.incoming().for_each(move |(socket, _addr)| {
        // channel for pushing subscriptions directly from subscriptions thread
        // to client socket
        let (subscriptions_tx, subscriptions_rx) = mpsc::unbounded::<Update>();

        let global_copy = global.clone();
        let state = Rc::new(RefCell::new(
            ThreadState::new(Arc::clone(&global), Arc::clone(&store), subscriptions_tx.clone())
        ));
        let state_clone = state.clone();

        match utils::init_dbs(&mut state.borrow_mut()) {
            Ok(()) => (),
            Err(_) => panic!("Cannot initialized db!"),
        };
        on_connect(&global_copy);

        // map incoming subscription updates to the same format as regular
        // responses so they can be processed in the same manner.
        let subscriptions = subscriptions_rx.map(|message| (
            Cow::from(""), ReturnType::string(vec![message].into_json())
        ));

        let (rdr, wtr) = socket.split();
        let lines = lines(BufReader::new(rdr));
        let responses = lines.map(move |line| {
            let line: Cow<str> = line.into();
            let resp = handler::gen_response(line.borrow(), &mut state.borrow_mut());
            (line, resp)
        });

        // merge responses and messages pushed directly by subscriptions updates
        // into a single stream
        let merged = subscriptions.select(responses.map_err(|_| ()));

        let writes = merged.fold(wtr, |wtr, (line, resp)| {
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
            write_all(wtr, buf).map(|(w, _)| w).map_err(|_| ())
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
