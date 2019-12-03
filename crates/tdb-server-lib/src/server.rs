use crate::prelude::*;
use byteorder::{BigEndian, ReadBytesExt};
use async_std::future;

// TODO: add onexit once async-std support is stablized
#[cfg(unix)]
#[allow(unused)]
async fn onexit(mut broker: Sender<Event>, settings: Arc<Settings>) {
    info!("`TERM` signal recieved; flushing all stores...");
    broker.send(Event::Command {from: None, command: Command::Flush(ReqCount::All)}).await.unwrap();
    info!("All stores flushed; calling plugin exit hooks...");
    crate::plugins::run_plugin_exit_hooks(broker, settings);
    info!("Plugin exit hooks called; exiting...");
    std::process::exit(0);
}

#[cfg(windows)]
#[allow(unused)]
fn onexit() { }


fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            error!("{}", e)
        }
    })
}

pub async fn run_server(host: &str, port: &str, settings: Arc<Settings>) -> Result<()> {
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

    let (broker_sender, broker_receiver) = mpsc::channel::<Event>(CHANNEL_SZ);

    // ctrlc::set_handler(move || {
    //     task::block_on(onexit(broker, settings));
    // });

    let broker = task::spawn(broker_loop(broker_receiver, Arc::clone(&settings)));
    let plugins = task::spawn(crate::plugins::run_plugins(broker_sender.clone(), settings.clone()));
    plugins.await;

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



async fn connection_loop(mut broker: Sender<Event>, stream: TcpStream) -> Result<()> {
    let stream = Arc::new(stream);
    let mut reader = BufReader::new(&*stream);
    let addr = stream.peer_addr()?;

    let (_shutdown_sender, shutdown_receiver) = mpsc::channel::<Void>(CHANNEL_SZ);
    broker
        .send(Event::NewConnection {
            addr: addr,
            stream: Arc::clone(&stream),
            shutdown: shutdown_receiver,
        })
        .await
        .unwrap();

    let mut bytes = [0; 4];
    let mut buf = Box::new([0; 65536*16]);
    while let Ok(()) = reader.read_exact(&mut bytes).await {
        let mut rdr = std::io::Cursor::new(bytes);
        let sz = rdr.read_u32::<BigEndian>().unwrap() as usize;
        bytes = rdr.into_inner();

        reader.read_exact(&mut buf[..sz]).await?;

        let command = crate::handler::parse_to_command(&buf[..sz]);
        let from = Some(addr);
        if let Err(_) = broker
            .send(Event::Command{from, command})
            .await
        {
            error!("unable to send event to broker");
            break;
        }
    }

    info!("Client dropped: {:?}", addr);

    Ok(())
}


async fn broker_loop(mut events: Receiver<Event>, settings: Arc<Settings>) {
    let (disconnect_sender, mut disconnect_receiver) = mpsc::channel::<(SocketAddr, Receiver<ReturnType>)>(1);

    let mut state = TectonicServer::new(settings);

    loop {
        let event = select! {
            event = events.next().fuse() => match event {
                None => break,
                Some(event) => event,
            },
            disconnect = disconnect_receiver.next().fuse() => {
                let (addr, _pending_messages) = disconnect.unwrap();
                assert!(state.connections.remove(&addr).is_some());
                let _ = state.unsub(&addr);

                continue;
            },
        };
        match event {
            Event::Command { from, command } => {
                state.command(command, from).await;
            },
            Event::History{} => {
                state.record_history();
            }
            Event::NewConnection { addr, stream, shutdown } => {
                let (client_sender, mut client_receiver) = mpsc::channel(2048);
                if state.new_connection(client_sender, addr) {
                    let mut disconnect_sender = disconnect_sender.clone();
                    // TODO: lift writer loop out so rx is passed in
                    spawn_and_log_error(async move {
                        let res = connection_writer_loop(&mut client_receiver, stream, shutdown).await;
                        disconnect_sender
                            .send((addr, client_receiver))
                            .await
                            .unwrap();
                        res
                    });

                }

            },
        }
    }
    error!("broker exited");
    drop(state);
    drop(disconnect_sender);
    while let Some((_name, _pending_messages)) = disconnect_receiver.next().await { }
}

async fn connection_writer_loop(
    messages: &mut Receiver<ReturnType>,
    stream: Arc<TcpStream>,
    mut shutdown: Receiver<Void>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(CHANNEL_SZ);
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
                if let Err(future::TimeoutError {..}) = future::timeout(
                    std::time::Duration::from_millis(0),
                    stream.write_all(&buf)
                ).await
                {
                    error!("tcpstream write_all timeout.");
                }
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
