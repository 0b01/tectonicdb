pub use crate::settings::{Settings, key_or_default, key_or_none};
pub use crate::state::{TectonicServer, Book};
pub use crate::handler::{ReturnType, Command, Event, Void, ReqCount, GetFormat, ReadLocation};
pub use crate::utils;
pub use tdb_core::dtf::{
    self,
    update::{Update, UpdateVecConvert},
};

pub use tdb_core::utils::within_range;

pub use std::path::Path;
pub use std::borrow::{Cow, Borrow};
pub use std::collections::hash_map::{Entry, HashMap};
pub use std::sync::Arc;

pub use futures::{FutureExt, SinkExt, StreamExt};
pub use futures::channel::mpsc::{self, Receiver, Sender};
pub use futures::channel::oneshot;
pub use async_std::{
    io::{BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    task,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub use arrayvec::ArrayString;
pub type BookName = ArrayString<64>;

pub use std::net::SocketAddr;

pub const CHANNEL_SZ: usize = 1024;