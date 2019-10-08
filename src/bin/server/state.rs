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
///

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
use crate::utils;
use crate::settings::Settings;
use crate::handler::{GetFormat, ReturnType, ReqCount, Loc, Range};
use crate::subscription::Subscriptions;

/// An atomic reference counter for accessing shared data.
pub type Global = Arc<RwLock<SharedState>>;
pub type HashMapStore<'a> = Arc<RwLock<HashMap<String, Store<'a>>>>;
pub type SubscriptionTX = futures::sync::mpsc::UnboundedSender<Update>;

#[derive(Debug)]
pub struct Store<'a> {
    pub name: Cow<'a, str>,
    pub fname: Cow<'a, str>,
    pub in_memory: bool,
    pub global: Global,
}

impl<'a> Store<'a> {
    /// push a new `update` into the vec
    pub fn add(&mut self, new_vec: Update) {
        let (is_autoflush) = {
            let mut wtr = self.global.write().unwrap();

            // send to insertion firehose
            {
                let tx = wtr.subs.lock().unwrap();
                let _ = tx.msg((self.name.to_string(), new_vec));
            }

            let is_autoflush = wtr.settings.autoflush;
            let flush_interval = wtr.settings.flush_interval;
            let _folder = wtr.settings.dtf_folder.to_owned();
            let name: &str = self.name.borrow();
            let vecs = wtr.vec_store.get_mut(name).expect(
                "KEY IS NOT IN HASHMAP",
            );

            vecs.0.push(new_vec);
            vecs.1 += 1;

            // Saves current store into disk after n items is inserted.
            let size = vecs.0.len(); // using the raw len so won't have race condition with load_size_from_file
            let is_autoflush = is_autoflush && size != 0 && (size as u32) % flush_interval == 0;

            if is_autoflush {
                info!(
                    "AUTOFLUSHING {}! Size: {} Last: {:?}",
                    self.name,
                    vecs.1,
                    vecs.0.last().clone().unwrap()
                );
            }

            is_autoflush
        };

        if is_autoflush {
            self.flush();
        }
    }

    pub fn count(&self) -> u64 {
        let rdr = self.global.read().unwrap();
        let name: &str = self.name.borrow();
        let vecs = rdr.vec_store.get(name).expect(
            "KEY IS NOT IN HASHMAP",
        );
        vecs.1
    }

    pub fn count_in_mem(&self) -> u64 {
        let rdr = self.global.read().unwrap();
        let name: &str = self.name.borrow();
        let vecs = rdr.vec_store.get(name).expect(
            "KEY IS NOT IN HASHMAP",
        );
        vecs.0.len() as u64
    }

    /// write items stored in memory into file
    /// If file exists, use append which only appends a filtered set of updates whose timestamp is larger than the old timestamp
    /// If file doesn't exists, simply encode.
    ///
    pub fn flush(&mut self) -> Option<bool> {
        {
            let mut rdr = self.global.write().unwrap(); // use a write lock to block write in client processes
            let folder = rdr.settings.dtf_folder.to_owned();
            let name: &str = self.name.borrow();
            let vecs = rdr.vec_store.get_mut(name).expect(
                "KEY IS NOT IN HASHMAP",
            );
            let fullfname = format!("{}/{}.dtf", &folder, self.fname);
            utils::create_dir_if_not_exist(&folder);

            let fpath = Path::new(&fullfname);
            let result = if fpath.exists() {
                dtf::file_format::append(&fullfname, &vecs.0)
            } else {
                dtf::file_format::encode(&fullfname, &self.name, &vecs.0)
            };
            match result {
                Ok(_) => info!("Successfully flushed."),
                Err(_) => error!("Error flushing file."),
            };

            // clear
            vecs.0.clear();
        }

        // continue clear
        self.in_memory = false;
        Some(true)
    }

    /// load items from dtf file
    fn load(&mut self) {
        let folder = self.global.read().unwrap().settings.dtf_folder.to_owned();
        let fname = format!("{}/{}.dtf", &folder, self.name);
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
                let mut wtr = self.global.write().unwrap();
                // let size = ups.len() as u64;
                let name: &str = self.name.borrow();
                let vecs = wtr.vec_store.get_mut(name).unwrap();
                vecs.0.append(&mut ups);
                // wtr.vec_store.insert(self.name.to_owned(), (ups, size));
                self.in_memory = true;
            }
        }
    }

    /// load size from file
    pub fn load_size_from_file(&mut self) {
        let header_size = {
            let rdr = self.global.read().unwrap();
            let folder = rdr.settings.dtf_folder.to_owned();
            let fname = format!("{}/{}.dtf", &folder, self.name);
            dtf::file_format::get_size(&fname)
        };
        match header_size {
            Ok(header_size) => {
                let mut wtr = self.global.write().unwrap();
                let name: &str = &self.name.borrow();
                wtr.vec_store
                    .get_mut(name)
                    .expect("Key is not in vec_store")
                    .1 = header_size;
            }
            Err(_) => {
                error!("Unable to read header size from file");
            }
        }
    }

    /// clear the vector. toggle in_memory. update size
    pub fn clear(&mut self) {
        {
            let mut rdr = self.global.write().unwrap();
            let name: &str = self.name.borrow();
            let vecs = (*rdr).vec_store.get_mut(name).expect(
                "KEY IS NOT IN HASHMAP",
            );
            vecs.0.clear();
            // vecs.1 = 0;
        }
        self.in_memory = false;
        self.load_size_from_file();
    }
}

/// Each client gets its own ThreadState
pub struct ThreadState<'thr, 'store> {
    /// Is client subscribe?
    pub is_subscribed: bool,
    /// current subscribed db
    pub subscribed_db: Option<String>,
    /// current receiver
    pub sub_id: Option<usize>,
    /// current receiver
    pub rx: Option<mpsc::Receiver<Update>>,

    pub subscription_tx: SubscriptionTX,

    /// mapping store_name -> Store
    pub store: Arc<RwLock<HashMap<String, Store<'store>>>>,

    /// the current STORE client is using
    pub current_store_name: Cow<'thr, str>,

    /// shared data
    pub global: Global,
}

macro_rules! current_store {
    ($self:ident, $fun:ident) => {
        {
            let name: &str = $self.current_store_name.borrow();
            let mut store = $self.store.write().unwrap();
            store.get_mut(name).expect(
                "KEY IS NOT IN HASHMAP",
            ).$fun()
        }
    };

    ($self:ident, $fun:ident, $param:ident) => {
        {
            let name: &str = $self.current_store_name.borrow();
            let mut store = $self.store.write().unwrap();
            store.get_mut(name).expect(
                "KEY IS NOT IN HASHMAP",
            ).$fun($param)
        }
    };
}

macro_rules! store {
    ($self:ident, $fun:ident) => {
        $self.store.write().unwrap().$fun()
    };
    ($self:ident, $fun:ident, $param:ident) => {
        $self.store.write().unwrap().$fun($param)
    };
}

impl<'thr, 'store> ThreadState<'thr, 'store> {
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
        let rdr = self.global.read().unwrap();
        let info_vec: Vec<String> = rdr.vec_store
            .iter()
            .map(|i| {
                let (key, value) = i;
                let vecs = &value.0;
                let size = value.1;
                format!(
                    r#"{{
    "name": "{}",
    "in_memory": {},
    "count": {}
  }}"#,
                    key,
                    !vecs.is_empty(),
                    size
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
    "total_count": {}
  }}"#,

            rdr.n_cxns,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            rdr.settings.autoflush,
            rdr.settings.flush_interval,
            rdr.settings.dtf_folder,
            rdr.vec_store.iter().fold(
                0,
                |acc, (_name, tup)| acc + tup.1,
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
        let rdr = self.global.read().unwrap();
        let objs: Vec<String> = (&rdr.history)
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
    pub fn insert(&mut self, up: Update, store_name: &str) -> Option<()> {
        match store!(self, get_mut, store_name) {
            Some(store) => {
                store.add(up);
                Some(())
            }
            None => None,
        }
    }

    /// Check if a table exists
    pub fn exists(&mut self, store_name: &str) -> bool {
        store!(self, contains_key, store_name)
    }

    /// Insert a row into current store.
    pub fn add(&mut self, up: Update) {
        current_store!(self, add, up);
    }

    pub fn set_autoflush(&mut self, is_autoflush: bool) {
        let mut global = self.global.write().unwrap();
        global.settings.autoflush = is_autoflush;
    }

    /// Create a new store
    pub fn create(&mut self, store_name: &str) {
        // insert a vector into shared hashmap
        {
            let mut global = self.global.write().unwrap();
            global.vec_store.insert(
                store_name.to_owned(),
                (Box::new(Vec::new()), 0),
            );
        }

        // insert a store into client state hashmap
        let store_name = String::from(store_name);
        self.store.write().unwrap().insert(
            store_name.clone(),
            Store {
                name: store_name.clone().into(),
                fname: store_name.into(),
                in_memory: false,
                global: self.global.clone(),
            },
        );
    }

    /// load a datastore file into memory
    pub fn use_db(&mut self, store_name: &str) -> Option<()> {
        if store!(self, contains_key, store_name) {
            self.current_store_name = store_name.to_owned().into();
            current_store!(self, load);
            Some(())
        } else {
            None
        }
    }

    /// return the count of the current store
    pub fn count(&mut self) -> u64 {
        current_store!(self, count)
    }

    /// return current store count in mem
    pub fn count_in_mem(&mut self) -> u64 {
        current_store!(self, count_in_mem)
    }

    /// Returns the total count
    pub fn countall_in_mem(&self) -> u64 {
        let rdr = self.global.read().unwrap();
        rdr.vec_store.iter().fold(
            0,
            |acc, (_name, tup)| acc + tup.0.len(),
        ) as u64
    }

    /// Returns the total count
    pub fn countall(&self) -> u64 {
        let rdr = self.global.read().unwrap();
        rdr.vec_store.iter().fold(
            0,
            |acc, (_name, tup)| acc + tup.1,
        )
    }

    pub fn sub(&mut self, dbname: &str) {
        self.is_subscribed = true;
        self.subscribed_db = Some(dbname.to_owned());
        let glb = self.global.read().unwrap();
        let (id, rx) = glb.subs.lock().unwrap()
            .sub(dbname.to_owned(), self.subscription_tx.clone());
        self.rx = Some(rx);
        self.sub_id = Some(id);
        info!("Subscribing to channel {}. id: {}", dbname, id);
    }

    pub fn unsub_all(&mut self) {
        let glb = self.global.read().unwrap();
        let _ = glb.subs.lock().unwrap().unsub_all();
    }

    /// unsubscribe
    pub fn unsub(&mut self) {
        if !self.is_subscribed {
            return;
        }
        let old_dbname = self.subscribed_db.clone().unwrap();
        let sub_id = self.sub_id.unwrap();

        let glb = self.global.read().unwrap();
        let _ = glb.subs.lock().unwrap().unsub(sub_id, &old_dbname);

        info!("Unsubscribing from channel {}. id: {}", old_dbname, sub_id);

        self.is_subscribed = false;
        self.subscribed_db = None;
        self.rx = None;
        self.sub_id = None;
    }

    /// remove everything in the current store
    pub fn clear(&mut self) {
        current_store!(self, clear);
    }

    /// remove everything in every store
    pub fn clearall(&mut self) {
        for store in store!(self, values_mut) {
            store.clear();
        }
    }

    /// save current store to file
    pub fn flush(&mut self) {
        current_store!(self, flush);
        // self.get_current_store().flush();
    }

    /// save all stores to corresponding files
    pub fn flushall(&mut self) {
        for store in store!(self, values_mut) {
            store.flush();
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
    pub fn get<'global, 'thread>(&'global mut self, count: ReqCount,
        format: GetFormat, range: Range, loc: Loc) -> Option<ReturnType<'thread>>
    {
        // return if requested 0 item
        if let ReqCount::Count(c) = count {
            if c == 0 {
                return None
            }
        }

        // check for items in memory
        let rdr = self.global.read().unwrap();
        let name: &str = self.current_store_name.borrow();
        let &(ref vecs, _) = rdr.vec_store.get(name)?;

        // if range, filter mem
        let acc = catch! {
            let (min_ts, max_ts) = range?;
            if !within_range(min_ts, max_ts, vecs.first()?.ts, vecs.last()?.ts) { return None; }
            Box::new(vecs.iter()
                .filter(|up| up.ts < max_ts && up.ts > min_ts)
                .map(|up| up.to_owned())
                .collect::<Vec<_>>()
            )
        }.unwrap_or(vecs.to_owned());

        // if only requested items in memory
        if let Loc::Mem = loc {
            return self.into_format(&acc, format);
        }

        // if count <= len, return
        if let ReqCount::Count(c) = count {
            if (c as usize) <= acc.len() {
                return self.into_format(&acc[..c as usize], format);
            }
        }

        // we need more items
        // check dtf files in folder and collect updates in requested range
        // and combine sequentially
        let mut ups_from_fs = acc;
        if let Some((min_ts, max_ts)) = range {
            let folder = {
                let rdr = self.global.read().unwrap();
                rdr.settings.dtf_folder.clone()
            };
            let ups = scan_files_for_range(&folder, &self.current_store_name, min_ts, max_ts);
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
            ReqCount::Count(c) => {
                if result.len() >= c as usize {
                    return self.into_format(&result[..(c as usize - 1)], format);
                } else {
                    return Some(ReturnType::Error(
                        format!("Requested {} but only have {}.", c, result.len()).into(),
                    ));
                }
            }
            ReqCount::All => self.into_format(&result, format),
        }
    }

    fn into_format<'thread, 'global>(&'global self, result: &[Update], format: GetFormat) -> Option<ReturnType<'thread>> {
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
                    Cow::Owned(format!("{}\n", result.as_csv())),
                )
            }
        };

        Some(ret)
    }

    /// create a new threadstate
    pub fn new<'a, 'b>(
        global: Global,
        store: HashMapStore<'b>,
        subscription_tx: SubscriptionTX,
    ) -> ThreadState<'a, 'b> {
        let dtf_folder: &str = &global.read().unwrap().settings.dtf_folder;
        let state = ThreadState {
            current_store_name: "default".into(),
            is_subscribed: false,
            subscribed_db: None,
            sub_id: None,
            rx: None,
            subscription_tx,
            store,
            global: global.clone(),
        };

        // insert default first, if there is a copy in memory this will be replaced
        let default_file = format!("{}/default.dtf", dtf_folder);
        let default_in_memory = !Path::new(&default_file).exists();
        state.store.write().unwrap().insert(
            "default".to_owned(),
            Store {
                name: "default".into(),
                fname: "default".into(),
                in_memory: default_in_memory,
                global: global.clone(),
            },
        );

        let rdr = global.read().unwrap();
        for (store_name, _vec) in &rdr.vec_store {
            let fname = format!("{}/{}.dtf", dtf_folder, store_name);
            let in_memory = !Path::new(&fname).exists();
            state.store.write().unwrap().insert(
                store_name.to_owned(),
                Store {
                    name: store_name.to_owned().into(),
                    fname: store_name.clone().into(),
                    in_memory: in_memory,
                    global: global.clone(),
                },
            );
        }
        state
    }
}

/// (updates, count)
pub type VecStore = (Box<Vec<Update>>, u64);

/// key: { btc_neo => [(t0, c0), (t1, c1), ...]
///        ...
///      { total => [...]}
pub type CountHistory = HashMap<String, CircularQueue<(SystemTime, u64)>>;

#[derive(Debug)]
pub struct SharedState {
    pub n_cxns: u16,
    pub settings: Settings,
    pub vec_store: HashMap<String, VecStore>,
    pub history: CountHistory,
    pub subs: Arc<Mutex<Subscriptions>>,
    pub rx: futures::sync::mpsc::UnboundedReceiver<Update>,
    pub subs_txs: HashMap<::std::thread::ThreadId, SubscriptionTX>,
}

impl SharedState {
    pub fn new(rx: futures::sync::mpsc::UnboundedReceiver<Update>, settings: Settings) -> SharedState {
        let mut hashmap = HashMap::new();
        hashmap.insert("default".to_owned(), (Box::new(Vec::new()), 0));
        let subs = Arc::new(Mutex::new(Subscriptions::new()));
        SharedState {
            n_cxns: 0,
            settings,
            vec_store: hashmap,
            history: HashMap::new(),
            rx,
            subs,
            subs_txs:  HashMap::new(),
        }
    }
}
