pub use clap::{Arg, App, ArgMatches};
pub use crate::settings::{Settings, key_or_default, key_or_none};
pub use libtectonic::dtf::update::{Update, UpdateVecConvert};
pub use crate::state::{GlobalState, Book};
pub use crate::handler::{ReturnType, Command};
pub use crate::handler::{Event, Void};
pub use crate::utils;

pub use std::{
    collections::hash_map::{Entry, HashMap},
    sync::Arc,
    rc::Rc,
};

// pub use crate::plugins::{run_plugins, run_plugin_exit_hooks};
pub use futures::{channel::mpsc, FutureExt, SinkExt};
pub use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};

pub use async_std::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    task,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type Sender<T> = mpsc::UnboundedSender<T>;
pub type Receiver<T> = mpsc::UnboundedReceiver<T>;

pub use std::net::SocketAddr;