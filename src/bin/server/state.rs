/// When client connects, the following happens:
///
/// 1. server creates a ThreadState
/// 2. initialize 'default' data store
/// 3. reads filenames under dtf_folder
/// 4. loads metadata but not updates
/// 5. client can retrieve server status using INFO command
///
/// When client adds some updates using ADD,
/// size increments and updates are added to memory
/// finally, call FLUSH to commit to disk the current store or FLUSH ALL to commit all available stores.
/// the client can free the updates from memory using CLEAR or CLEARALL

use crate::prelude::*;

macro_rules! catch {
    ($($code:tt)*) => {
        (|| { Some({ $($code)* }) })()
    }
}

use circular_queue::CircularQueue;
use futures;
use libtectonic::dtf::{self, update::{Update, UpdateVecConvert}};
use libtectonic::storage::utils::scan_files_for_range;
use libtectonic::utils::within_range;

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock, Mutex, mpsc};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::settings::Settings;
use crate::handler::{GetFormat, ReturnType, ReqCount, ReadLocation, Range};
use crate::subscription::Subscriptions;


pub fn into_format(result: &[Update], format: &GetFormat) -> Option<ReturnType> {
    let ret = match format {
        GetFormat::Dtf => {
            let mut bytes: Vec<u8> = Vec::new();
            let _ = dtf::file_format::write_batches(&mut bytes, &result);
            ReturnType::Bytes(bytes)
        }
        GetFormat::Json => {
            ReturnType::String(
                Cow::Owned(format!("[{}]\n", result.as_json()))
            )
        }
        GetFormat::Csv => {
            ReturnType::String(
                Cow::Owned(format!("{}\n", result.as_csv()))
            )
        }
    };
    Some(ret)
}



/// (updates, nominal count)
pub struct Book {
    pub vec: Vec<Update>,
    pub nominal_count: u64,
    pub name: String,
    pub in_memory: bool,
    pub settings: Settings,
}

impl Book {

    pub fn new(name: &str, settings: Settings) -> Self {
        let vec = vec![];
        let nominal_count = 0;
        let name = name.to_owned();
        let in_memory = false;
        let mut ret = Self {
            vec,
            nominal_count,
            name,
            in_memory,
            settings,
        };
        ret.load();
        ret
    }

    /// load items from dtf file
    fn load(&mut self) {
        let fname = format!("{}/{}.dtf", &self.settings.dtf_folder, self.name);
        if Path::new(&fname).exists() && !self.in_memory {
            // let file_item_count = dtf::read_meta(&fname).nums;
            // // when we have more items in memory, don't load
            // if file_item_count < self.count() {
            //     warn!("There are more items in memory than in file. Cannot load from file.");
            //     return;
            // }
            let ups = dtf::file_format::decode(&fname, None);
            if ups.is_err() {
                error!("Unable to decode file during load!");
                return;
            } else {
                let mut ups = ups.unwrap();
                // let size = ups.len() as u64;
                let name: &str = self.name.borrow();
                self.vec.append(&mut ups);
                // wtr.vec_store.insert(self.name.to_owned(), (ups, size));
                self.in_memory = true;
            }
        }
    }

    /// load size from file
    pub fn load_size_from_file(&mut self) {
        let header_size = {
            let fname = format!("{}/{}.dtf", &self.settings.dtf_folder, self.name);
            dtf::file_format::get_size(&fname)
        };
        match header_size {
            Ok(header_size) => {
                self.nominal_count = header_size;
            }
            Err(_) => {
                error!("Unable to read header size from file");
            }
        }
    }

    fn add(&mut self, up: Update) {
        let name: &str = self.name.borrow();

        self.vec.push(up);
        // self.nominal_count += 1; // don't increment

        // Saves current store into disk after n items is inserted.
        let len = self.vec.len() as u32;
        if self.settings.autoflush && len != 0 && len % self.settings.flush_interval == 0 {
            info!(
                "AUTOFLUSHING {}! Size: {}",
                self.name,
                len,
            );
            self.flush();
        }
    }

    fn flush(&mut self) -> Option<()> {

        let name: &str = self.name.borrow();

        let fullfname = format!("{}/{}.dtf", &self.settings.dtf_folder, self.name);
        utils::create_dir_if_not_exist(&self.settings.dtf_folder);

        let fpath = Path::new(&fullfname);
        let result = if fpath.exists() {
            dtf::file_format::append(&fullfname, &self.vec)
        } else {
            dtf::file_format::encode(&fullfname, &self.name, &self.vec)
        };
        match result {
            Ok(_) => info!("Successfully flushed."),
            Err(_) => error!("Error flushing file."),
        };

        self.vec.clear();
        self.in_memory = false;

        Some(())
    }
}


/// key: { btc_neo => [(t0, c0), (t1, c1), ...]
///        ...
///      { total => [...]}
pub type CountHistory = HashMap<String, CircularQueue<(SystemTime, u64)>>;

/// Each client gets its own ThreadState
pub struct Connection {
    pub outbound: Sender<ReturnType>,
    // pub subscription_tx: SubscriptionTX,

    /// the current Store client is using
    pub book_name: String,
}

impl Connection {
    pub fn new(outbound: Sender<ReturnType>) -> Self {
        Self {
            outbound,
            book_name: "default".into(),
        }
    }
}

pub struct GlobalState {
    pub connections: HashMap<SocketAddr, Connection>,
    pub settings: Settings,
    pub books: HashMap<String, Book>,
    pub history: CountHistory,
    // pub subs: Arc<Mutex<Subscriptions>>,
}

impl GlobalState {
    pub fn new(settings: Settings) -> Self {
        let mut connections = HashMap::new();
        let mut books = HashMap::new();
        books.insert(
            "default".to_owned(),
            Book::new("default", settings.clone())
        );
        // let subs = Arc::new(Mutex::new(Subscriptions::new()));
        let history = HashMap::new();
        Self {
            settings,
            books,
            history,
            // subs,
            connections,
        }
    }

    pub async fn process_command(&mut self, command: &Command, sock: &SocketAddr) -> ReturnType {
        use Command::*;
        match &command {
            Noop => ReturnType::string(""),
            Ping => ReturnType::string("PONG"),
            Help => ReturnType::string(ReturnType::HELP_STR),
            Info => ReturnType::string(self.info()),
            Perf => ReturnType::string(self.perf()),
            Count(ReqCount::Count(_), ReadLocation::Fs) => {
                self.count(sock)
                    .map(|c| ReturnType::string(format!("{}", c)))
                    .unwrap_or_else(|| ReturnType::error("Unable to get count"))
            },
            Count(ReqCount::Count(_), ReadLocation::Mem) => {
                self.count_in_mem(sock)
                    .map(|c| ReturnType::string(format!("{}", c)))
                    .unwrap_or_else(|| ReturnType::error("Unable to get count in memory"))
            },
            Count(ReqCount::All, ReadLocation::Fs) => ReturnType::string(format!("{}", self.countall())),
            Count(ReqCount::All, ReadLocation::Mem) => ReturnType::string(format!("{}", self.countall_in_mem())),
            Clear(ReqCount::Count(_)) => {
                self.clear(sock);
                ReturnType::ok()
            }
            Clear(ReqCount::All) => {
                self.clearall();
                ReturnType::ok()
            }
            Flush(ReqCount::Count(_)) => {
                self.flush(sock);
                ReturnType::ok()
            }
            Flush(ReqCount::All) => {
                self.flushall(sock);
                ReturnType::ok()
            }
            AutoFlush(is_autoflush) =>  {
                self.set_autoflush(*is_autoflush);
                ReturnType::ok()
            }
            // update, dbname
            Insert(Some(up), Some(dbname)) => {
                match self.insert(*up, dbname.as_str()) {
                    Some(()) => ReturnType::string(""),
                    None => ReturnType::Error(Cow::Owned(format!("DB {} not found.", &dbname))),
                }
            }
            Insert(Some(up), None) => {
                let book_name = self.conn(sock).unwrap().book_name.clone();
                match self.insert(*up, book_name.as_str()) {
                    Some(()) => ReturnType::string(""),
                    None => ReturnType::Error(Cow::Owned(format!("DB {} not found.", &book_name))),
                }
            }
            Insert(None, _) => ReturnType::error("Unable to parse line"),
            Create(dbname) => {
                self.create(&dbname);
                ReturnType::string(format!("Created DB `{}`.", &dbname))
            }
            Subscribe(dbname) => {
                self.sub(&dbname);
                ReturnType::string(format!("Subscribed to {}", dbname))
            }
            // Subscription => {
            //     let message = state.rx.as_ref().unwrap().try_recv();
            //     match message {
            //         Ok(msg) => ReturnType::string([msg].as_json()),
            //         _ => ReturnType::string("NONE"),
            //     }
            // }
            // Unsubscribe(ReqCount::All) => {
            //     self.unsub_all();
            //     ReturnType::string("Unsubscribed everything!")
            // }
            // Unsubscribe(ReqCount::Count(_)) => {
            //     let old_dbname = state.subscribed_db.clone().unwrap();
            //     self.unsub();
            //     ReturnType::string(format!("Unsubscribed from {}", old_dbname))
            // }
            Use(dbname) => {
                match self.use_db(&dbname, sock) {
                    Some(_) => ReturnType::string(format!("SWITCHED TO DB `{}`.", &dbname)),
                    None => ReturnType::error(format!("No db named `{}`", dbname)),
                }
            }
            Exists(dbname) => {
                if self.exists(&dbname) {
                    ReturnType::ok()
                } else {
                    ReturnType::error(format!("No db named `{}`", dbname))
                }
            }
            Get(cnt, fmt, rng, loc) =>
                self.get(cnt, fmt, *rng, loc, sock)
                    .unwrap_or(ReturnType::error("Not enough items to return")),
            Unknown => ReturnType::error("Unknown command."),
        }
    }

    /// Get information about the server
    ///
    /// Returns a JSON string.
    ///
    /// {
    ///     "meta":
    ///     {
    ///         "cxns": 10 // current number of connected clients
    ///     },
    ///     "stores":
    ///     {
    ///         "name": "something", // name of the store
    ///         "in_memory": true, // if the file is read into memory
    ///         "count": 10 // number of rows in this store
    ///     }
    /// }
    pub fn info(&self) -> String {
        let info_vec: Vec<String> = self.books
            .iter()
            .map(|i| {
                let (key, book) = i;
                format!(
                    r#"{{
    "name": "{}",
    "in_memory": {},
    "count": {}
  }}"#,
                    key,
                    book.vec.len(),
                    book.nominal_count,
                )
            })
            .collect();
        let metadata = format!(
            r#"{{
    "cxns": {},
    "ts": {},
    "autoflush_enabled": {},
    "autoflush_interval": {},
    "dtf_folder": "{}",
    "total_in_memory_count": {},
    "total_count": {}
  }}"#,
            self.connections.len(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            self.settings.autoflush,
            self.settings.flush_interval,
            self.settings.dtf_folder,
            self.books.iter().fold(
                0,
                |acc, (_name, tup)| acc + tup.vec.len(),
            ),
            self.books.iter().fold(
                0,
                |acc, (_name, tup)| acc + tup.nominal_count,
            )
        );
        let mut ret = format!(
            r#"{{
  "meta": {},
  "dbs": [{}]
}}"#,
            metadata,
            info_vec.join(", ")
        );
        ret.push('\n');
        ret
    }

    /// Returns a JSON object like
    /// [{"total": [1508968738: 0]}, {"default": [1508968738: 0]}]
    pub fn perf(&self) -> String {
        let objs: Vec<String> = (&self.history)
            .iter()
            .map(|(name, vec)| {
                let hists: Vec<String> = vec.iter()
                    .map(|&(t, size)| {
                        let ts = t.duration_since(UNIX_EPOCH).unwrap().as_secs();
                        format!("\"{}\":{}", ts, size)
                    })
                    .collect();
                format!(r#"{{"{}": {{{}}}}}"#, name, hists.join(", "))
            })
            .collect();

        format!("[{}]\n", objs.join(", "))
    }

    /// Insert a row into store
    pub fn insert(&mut self, up: Update, book_name: &str) -> Option<()> {
        match self.books.get_mut(book_name) {
            Some(mut store) => {
                store.add(up);
                Some(())
            }
            None => None,
        }
    }

    /// Check if a table exists
    pub fn exists(&mut self, book_name: &str) -> bool {
        self.books.contains_key(book_name)
    }

    pub fn set_autoflush(&mut self, autoflush: bool) {
        self.settings.autoflush = autoflush;
    }

    /// Create a new store
    pub fn create(&mut self, book_name: &str) {
        // insert a vector into shared hashmap
        self.books.insert(
            book_name.to_owned(),
            Book::new(book_name, self.settings.clone()),
        );
    }

    /// load a datastore file into memory
    pub fn use_db(&mut self, book_name: &str, sock: &SocketAddr) -> Option<()> {
        if self.books.contains_key(book_name) {
            self.conn_mut(sock)?.book_name = book_name.to_string();
            self.book_mut(sock)?.load();
            Some(())
        } else {
            None
        }
    }

    /// return the count of the current store
    pub fn count(&mut self, sock: &SocketAddr) -> Option<u64> {
        let ret = self.book(sock)?.nominal_count;
        Some(ret)
    }

    /// return current store count in mem
    pub fn count_in_mem(&mut self, sock: &SocketAddr) -> Option<u64> {
        let ret = self.book(sock)?.vec.len() as u64;
        Some(ret)
    }

    /// Returns the total count
    pub fn countall_in_mem(&self) -> u64 {
        self.books.values().fold(
            0,
            |acc, book| acc + book.vec.len(),
        ) as u64
    }

    /// Returns the total count
    pub fn countall(&self) -> u64 {
        self.books.values().fold(
            0,
            |acc, book| acc + book.nominal_count,
        )
    }

    pub fn sub(&mut self, dbname: &str) {
        // self.is_subscribed = true;
        // self.subscribed_db = Some(dbname.to_owned());
        // let glb = self.global.read().unwrap();
        // let (id, rx) = glb.subs.lock().unwrap()
        //     .sub(dbname.to_owned(), self.subscription_tx.clone());
        // self.rx = Some(rx);
        // self.sub_id = Some(id);
        // info!("Subscribing to channel {}. id: {}", dbname, id);
    }

    pub fn unsub_all(&mut self) {
        // let glb = self.global.read().unwrap();
        // let _ = glb.subs.lock().unwrap().unsub_all();
    }

    /// unsubscribe
    pub fn unsub(&mut self) {
        // if !self.is_subscribed {
        //     return;
        // }
        // let old_dbname = self.subscribed_db.clone().unwrap();
        // let sub_id = self.sub_id.unwrap();

        // let glb = self.global.read().unwrap();
        // let _ = glb.subs.lock().unwrap().unsub(sub_id, &old_dbname);

        // info!("Unsubscribing from channel {}. id: {}", old_dbname, sub_id);

        // self.is_subscribed = false;
        // self.subscribed_db = None;
        // self.rx = None;
        // self.sub_id = None;
    }

    /// remove everything in the current store
    pub fn clear(&mut self, sock: &SocketAddr) -> Option<()> {
        let book = self.book_mut(sock)?;
        book.vec.clear();
        // vecs.1 = 0;
        book.in_memory = false;
        book.load_size_from_file();
        Some(())
    }

    /// remove everything in every store
    pub fn clearall(&mut self) {
        for book in self.books.values_mut() {
            book.vec.clear();
            // vecs.1 = 0;
            book.in_memory = false;
            book.load_size_from_file();
        }
    }

    /// write items stored in memory into file
    /// If file exists, use append which only appends a filtered set of updates whose timestamp is larger than the old timestamp
    /// If file doesn't exists, simply encode.
    ///
    pub fn flush(&mut self, sock: &SocketAddr) -> Option<()> {
        self.book_mut(&sock)?.flush()
    }

    /// save all stores to corresponding files
    pub fn flushall(&mut self, sock: &SocketAddr) {
        for book in self.books.values_mut() {
            book.flush();
        }
    }

    /// get `count` items from the current store
    ///
    /// return if request item,
    /// get from mem
    /// if range, filter
    /// if count <= len, return
    /// need more, get from fs
    ///
    pub fn get(&self, count: &ReqCount, format: &GetFormat, range: Range, loc: &ReadLocation, sock: &SocketAddr)
        -> Option<ReturnType>
    {
        // return if requested 0 item
        if let ReqCount::Count(c) = count {
            if *c == 0 {
                return None
            }
        }

        let book = self.book(sock)?;

        // if range, filter mem
        let acc = catch! {
            let (min_ts, max_ts) = range?;
            if !within_range(min_ts, max_ts, book.vec.first()?.ts, book.vec.last()?.ts) { return None; }
            book.vec.iter()
                .filter(|up| up.ts < max_ts && up.ts > min_ts)
                .map(|up| up.to_owned())
                .collect::<Vec<_>>()
        }.unwrap_or(book.vec.to_owned());

        // if only requested items in memory
        if let ReadLocation::Mem = loc {
            return into_format(&acc, format);
        }

        // if count <= len, return
        if let &ReqCount::Count(c) = count {
            if (c as usize) <= acc.len() {
                return into_format(&acc[..c as usize], format);
            }
        }

        // we need more items
        // check dtf files in folder and collect updates in requested range
        // and combine sequentially
        let mut ups_from_fs = acc;
        if let Some((min_ts, max_ts)) = range {
            let folder = {
                self.settings.dtf_folder.clone()
            };
            let ups = scan_files_for_range(&folder, &self.conn(sock)?.book_name, min_ts, max_ts);
            match ups {
                Ok(ups) => {
                    ups_from_fs.extend(ups);
                }
                Err(_) => {
                    error!("Unable to scan files for range.");
                }
            }
        }

        let result = ups_from_fs;

        match count {
            &ReqCount::Count(c) => {
                if result.len() >= c as usize {
                    return into_format(&result[..(c as usize - 1)], &format);
                } else {
                    return Some(ReturnType::Error(
                        format!("Requested {} but only have {}.", c, result.len()).into(),
                    ));
                }
            }
            ReqCount::All => into_format(&result, &format),
        }
    }

    pub fn new_connection(&mut self, client_sender: Sender<ReturnType>, sock: SocketAddr) -> bool {
        match self.connections.entry(sock.clone()) {
            Entry::Occupied(..) => false,
            Entry::Vacant(entry) => {
                entry.insert(Connection::new(client_sender));
                true
            }
        }
    }

    pub async fn command(&mut self, cmd: &Command, sock: &SocketAddr) -> Result<()> {
        if !self.connections.contains_key(sock) {
            return Ok(())
        }
        let ret = self.process_command(cmd, sock).await;
        self.connections.get_mut(sock).unwrap().outbound.send(ret).await.unwrap();
        Ok(())
    }

    pub fn conn(&self, sock: &SocketAddr) -> Option<&Connection> {
        self.connections.get(sock)
    }

    pub fn conn_mut(&mut self, sock: &SocketAddr) -> Option<&mut Connection> {
        self.connections.get_mut(sock)
    }

    pub fn book_mut(&mut self, sock: &SocketAddr) -> Option<&mut Book> {
        let book_name = self.conn(sock)?.book_name.clone();
        self.books.get_mut(&book_name)
    }

    pub fn book(&self, sock: &SocketAddr) -> Option<&Book> {
        let book_name = self.conn(sock)?.book_name.clone();
        self.books.get(&book_name)
    }
}