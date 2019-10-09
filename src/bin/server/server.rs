use crate::prelude::*;

use byteorder::{WriteBytesExt, NetworkEndian};

use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::str;
use std::sync::{Arc, RwLock};
use std::process::exit;

// #[cfg(unix)]
// fn enable_platform_hook(
//     handle: &Handle,
//     global: Global,
//     store: HashMapStore<'static>) {
//     let (subscriptions_tx, _) = mpsc::unbounded::<Update>();
//     let mut state = ThreadState::new(global, store, subscriptions_tx);

//     // Creates a listener for Unix signals that takes care of flushing all stores to file before
//     // shutting down the server.
//     // Catches `TERM` signals, which are sent by Kubernetes during graceful shutdown.
//     let signal_handler = tokio_signal::unix::Signal::new(15)
//         .flatten_stream()
//         .for_each(move |signal| {
//             println!("Signal: {}", signal);
//             info!("`TERM` signal recieved; flushing all stores...");
//             info!("All stores flushed; calling plugin exit hooks...");
//             run_plugin_exit_hooks(&state);
//             info!("Plugin exit hooks called; exiting...");
//             state.flushall();
//             exit(0);

//             #[allow(unreachable_code)]
//             Ok(())
//         })
//         .map_err(|err| error!("Error in signal handler future: {:?}", err));

//     handle.spawn(signal_handler);
// }

// #[cfg(windows)]
// fn enable_platform_hook<'a>(
//     handle: &Handle,
//     global: Global,
//     store: HashMapStore<'a>
// ) {
// }


fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}


async fn connection_loop(mut broker: Sender<Event>, stream: TcpStream) -> Result<()> {
    let stream = Arc::new(stream);
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();

    let (_shutdown_sender, shutdown_receiver) = mpsc::unbounded::<Void>();
    broker
        .send(Event::NewPeer {
            sock: stream.peer_addr()?.clone(),
            stream: Arc::clone(&stream),
            shutdown: shutdown_receiver,
        })
        .await
        .unwrap();

    while let Some(line) = lines.next().await {
        let command = crate::handler::parse_to_command(&line?);
        let from = stream.peer_addr()?.clone();
        broker
            .send(Event::Command{from, command})
            .await
            .unwrap();
    }

    Ok(())
}


async fn broker_loop(mut events: Receiver<Event>, settings: Settings) {
    let (disconnect_sender, mut disconnect_receiver) = mpsc::unbounded::<(SocketAddr, Receiver<ReturnType>)>();

    let mut state = GlobalState::new(settings);

    loop {
        let event = select! {
            event = events.next().fuse() => match event {
                None => break,
                Some(event) => event,
            },
            disconnect = disconnect_receiver.next().fuse() => {
                let (name, _pending_messages) = disconnect.unwrap();
                assert!(state.connections.remove(&name).is_some());
                continue;
            },
        };
        match event {
            Event::Command { from, command } => {
                state.command(&command, &from).await;
            },
            Event::History{} => {
                state.record_history();
            }
            Event::NewPeer {
                sock,
                stream,
                shutdown,
            } => {
                let (client_sender, mut client_receiver) = mpsc::unbounded();
                if state.new_connection(client_sender, sock) {
                    let mut disconnect_sender = disconnect_sender.clone();
                    spawn_and_log_error(async move {
                        let res = connection_writer_loop(&mut client_receiver, stream, shutdown).await;
                        disconnect_sender
                            .send((sock, client_receiver))
                            .await
                            .unwrap();
                        res
                    });

                }

            },
        }
    }
    drop(state);
    drop(disconnect_sender);
    while let Some((_name, _pending_messages)) = disconnect_receiver.next().await {}
}

pub async fn run_server(host: &str, port: &str, settings: Settings) -> Result<()> {
    let addr = format!("{}:{}", host, port);
    let addr: SocketAddr = addr.parse().expect("Invalid host or port provided!");

    info!("Trying to bind to addr: {}", addr);
    if !settings.autoflush {
        warn!("Autoflush is off!");
    }
    info!(
        "Autoflush is {}: every {} inserts.",
        settings.autoflush,
        settings.flush_interval
    );
    info!("History granularity: {}.", settings.granularity);

    let listener = TcpListener::bind(addr).await?;

    let (broker_sender, broker_receiver) = mpsc::unbounded::<Event>();
    let broker = task::spawn(broker_loop(broker_receiver, settings.clone()));
    task::spawn(run_plugins(broker_sender.clone(), settings.clone())).await;

    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        info!("Accepting from: {}", stream.peer_addr()?);
        spawn_and_log_error(connection_loop(broker_sender.clone(), stream));
    }
    drop(broker_sender);
    broker.await;
    Ok(())
}


async fn connection_writer_loop(
    messages: &mut Receiver<ReturnType>,
    stream: Arc<TcpStream>,
    mut shutdown: Receiver<Void>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(1000);
    let mut stream = &*stream;
    loop {
        select! {
            msg = messages.next().fuse() => {
                match msg {
                    Some(ReturnType::Bytes(bytes)) => {
                        buf.write(&[0x1]).await?;
                        buf.write(&bytes.len().to_be_bytes()).await?;
                        buf.write(&bytes).await?;
                        // buf.flush().await?;
                    },
                    Some(ReturnType::String(str_resp)) => {
                        buf.write(&[0x1]).await?;
                        buf.write(&str_resp.len().to_be_bytes()).await?;
                        buf.write(&str_resp.as_bytes()).await?;
                        // buf.flush().await?;
                    },
                    Some(ReturnType::Error(errmsg)) => {
                        // error!("Req: `{}`", line);
                        // error!("Err: `{}`", errmsg.clone());
                        buf.write(&[0x0]).await?;
                        let ret = format!("ERR: {}\n", errmsg);
                        buf.write(&ret.len().to_be_bytes()).await?;
                        buf.write(ret.as_bytes()).await?;
                        // buf.flush().await?;
                    },
                    None => break,
                };
                stream.write_all(&buf).await?;
                buf.clear()
            },
            void = shutdown.next().fuse() => match void {
                Some(void) => match void {},
                None => break,
            }
        }
    }
    Ok(())
}
