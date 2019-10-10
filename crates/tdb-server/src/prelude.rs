pub use crate::settings::{Settings, key_or_default, key_or_none};
pub use crate::state::{TectonicServer, Book};
pub use crate::handler::{ReturnType, Command, Event, Void, ReqCount, GetFormat, ReadLocation};
pub use crate::utils;
pub use libtectonic::dtf::{
    self,
    update::{Update, UpdateVecConvert},
};

pub use libtectonic::utils::within_range;

pub use std::path::Path;
pub use std::borrow::{Cow, Borrow};
pub use std::collections::hash_map::{Entry, HashMap};
pub use std::sync::Arc;

pub use futures::{FutureExt, SinkExt};
pub use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
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