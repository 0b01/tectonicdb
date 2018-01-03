macro_rules! catch {
    ($($code:tt)*) => {
        (|| { Some({ $($code)* }) })()
    }
}


use libtectonic;
use libtectonic::dtf;
use libtectonic::dtf::update::Update;
use libtectonic::storage::utils::scan_files_for_range;

use std::collections::HashMap;
use utils;
use std::path::Path;
use settings::Settings;
use std::sync::{Arc, RwLock, Mutex, mpsc};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use handler::{GetFormat, ReturnType, ReqCount, Loc, Range};
use subscription::Subscriptions;

/// name: *should* be the filename
/// in_memory: are the updates read into memory?
/// size: true number of items
/// v: vector of updates
///
///
/// When client connects, the following happens:
///
/// 1. server creates a State
/// 2. initialize 'default' data store
/// 3. reads filenames under dtf_folder
/// 4. loads metadata but not updates
/// 5. client can retrieve server status using INFO command
///
/// When client adds some updates using ADD or BULKADD,
/// size increments and updates are added to memory
/// finally, call FLUSH to commit to disk the current store or FLUSHALL to commit all available stores.
/// the client can free the updates from memory using CLEAR or CLEARALL
///
#[derive(Debug)]
pub struct Store {
    pub name: String,
    pub fname: String,
    pub in_memory: bool,
    pub global: Global,
}

/// An atomic reference counter for accessing shared data.
pub type Global = Arc<RwLock<SharedState>>;

impl Store {
    /// push a new `update` into the vec
    pub fn add(&mut self, new_vec: Update) {
        let is_autoflush = {
            let mut wtr = self.global.write().unwrap();

            // send to insertion firehose
            {
                let tx = wtr.subs.lock().unwrap();
                let _ = tx.msg(Arc::new(Mutex::new((self.name.clone(), new_vec))));
            }

            let is_autoflush = wtr.settings.autoflush;
            let flush_interval = wtr.settings.flush_interval;
            let _folder = wtr.settings.dtf_folder.to_owned();
            let vecs = wtr.vec_store.get_mut(&self.name).expect(
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
        let vecs = rdr.vec_store.get(&self.name).expect(
            "KEY IS NOT IN HASHMAP",
        );
        vecs.1
    }

    /// write items stored in memory into file
    /// If file exists, use append which only appends a filtered set of updates whose timestamp is larger than the old timestamp
    /// If file doesn't exists, simply encode.
    ///
    pub fn flush(&mut self) -> Option<bool> {
        {
            let mut rdr = self.global.write().unwrap(); // use a write lock to block write in client processes
            let folder = rdr.settings.dtf_folder.to_owned();
            let vecs = rdr.vec_store.get_mut(&self.name).expect(
                "KEY IS NOT IN HASHMAP",
            );
            let fullfname = format!("{}/{}.dtf", &folder, self.fname);
            utils::create_dir_if_not_exist(&folder);

            let fpath = Path::new(&fullfname);
            let result = if fpath.exists() {
                dtf::append(&fullfname, &vecs.0)
            } else {
                dtf::encode(&fullfname, &self.name, &vecs.0)
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
            let ups = dtf::decode(&fname, None);
            if ups.is_err() {
                error!("Unable to decode file during load!");
                return;
            } else {
                let mut ups = ups.unwrap();
                let mut wtr = self.global.write().unwrap();
                // let size = ups.len() as u64;
                let vecs = wtr.vec_store.get_mut(&self.name).unwrap();
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
            dtf::get_size(&fname)
        };
        match header_size {
            Ok(header_size) => {
                let mut wtr = self.global.write().unwrap();
                wtr.vec_store
                    .get_mut(&self.name)
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
            let vecs = (*rdr).vec_store.get_mut(&self.name).expect(
                "KEY IS NOT IN HASHMAP",
            );
            vecs.0.clear();
            // vecs.1 = 0;
        }
        self.in_memory = false;
        self.load_size_from_file();
    }
}

/// Each client gets its own State
pub struct State {
    /// Is inside a BULKADD operation?
    pub is_adding: bool,
    /// Current selected db using `BULKADD INTO [db]`
    pub bulkadd_db: Option<String>,

    /// Is client subscribe?
    pub is_subscribed: bool,
    /// current subscribed db
    pub subscribed_db: Option<String>,
    /// current receiver
    pub sub_id: Option<usize>,
    /// current receiver
    pub rx: Option<Arc<Mutex<mpsc::Receiver<Update>>>>,

    /// mapping store_name -> Store
    pub store: HashMap<String, Store>,

    /// the current STORE client is using
    pub current_store_name: String,

    /// shared data
    pub global: Global,
}

impl State {
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
        match self.store.get_mut(store_name) {
            Some(store) => {
                store.add(up);
                Some(())
            }
            None => None,
        }
    }

    /// Check if a table exists
    pub fn exists(&mut self, store_name: &str) -> bool {
        self.store.contains_key(store_name)
    }

    /// Insert a row into current store.
    pub fn add(&mut self, up: Update) {
        let current_store = self.get_current_store();
        current_store.add(up);
    }


    /// Create a new store
    pub fn create(&mut self, store_name: &str) {
        // insert a vector into shared hashmap
        {
            let mut global = self.global.write().unwrap();
            global.vec_store.insert(
                store_name.to_owned(),
                (box Vec::new(), 0),
            );
        }
        // insert a store into client state hashmap
        self.store.insert(
            store_name.to_owned(),
            Store {
                name: store_name.to_owned(),
                fname: format!("{}--{}", Uuid::new_v4(), store_name),
                in_memory: false,
                global: self.global.clone(),
            },
        );
    }

    /// load a datastore file into memory
    pub fn use_db(&mut self, store_name: &str) -> Option<()> {
        if self.store.contains_key(store_name) {
            self.current_store_name = store_name.to_owned();
            let current_store = self.get_current_store();
            current_store.load();
            Some(())
        } else {
            None
        }
    }

    /// return the count of the current store
    pub fn count(&mut self) -> u64 {
        let store = self.get_current_store();
        store.count()
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
        let (id, rx) = glb.subs.lock().unwrap().sub(dbname.to_owned());
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
        self.get_current_store().clear();
    }

    /// remove everything in every store
    pub fn clearall(&mut self) {
        for store in self.store.values_mut() {
            store.clear();
        }
    }

    /// save current store to file
    pub fn flush(&mut self) {
        self.get_current_store().flush();
    }

    /// save all stores to corresponding files
    pub fn flushall(&mut self) {
        for store in self.store.values_mut() {
            store.flush();
        }
    }

    /// returns the current store as a mutable reference
    fn get_current_store(&mut self) -> &mut Store {
        self.store.get_mut(&self.current_store_name).expect(
            "KEY IS NOT IN HASHMAP",
        )
    }

    /// get `count` items from the current store
    ///
    /// return if request item,
    /// get from mem
    /// if range, filter
    /// if count <= len, return
    /// need more, get from fs
    ///
    pub fn get(
        &mut self,
        count: ReqCount,
        format: GetFormat,
        range: Range,
        loc: Loc,
    ) -> Option<ReturnType> {
        // return if requested 0 item
        if let ReqCount::Count(c) = count {
            if c == 0 {
                return None;
            }
        }

        // check for items in memory
        let rdr = self.global.read().unwrap();
        let &(ref vecs, _) = rdr.vec_store.get(&self.current_store_name)?;

        // if range, filter mem
        let acc = catch! {
            let (min_ts, max_ts) = range?;
            if !libtectonic::utils::within_range(min_ts, max_ts, vecs.first()?.ts, vecs.last()?.ts) { return None; }
            box vecs.iter()
                .filter(|up| up.ts < max_ts && up.ts > min_ts)
                .map(|up| up.to_owned())
                .collect::<Vec<_>>()
        }.unwrap_or(vecs.to_owned());

        // if only requested items in memory
        if let Loc::Mem = loc {
            return self._return_aux(&acc, format);
        }

        // if count <= len, return
        if let ReqCount::Count(c) = count {
            if (c as usize) <= acc.len() {
                return self._return_aux(&acc[..c as usize], format);
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
                    return self._return_aux(&result[..(c as usize - 1)], format);
                } else {
                    return Some(ReturnType::Error(
                        format!("Requested {} but only have {}.", c, result.len()),
                    ));
                }
            }
            ReqCount::All => self._return_aux(&result, format),
        }
    }

    fn _return_aux(&self, result: &[Update], format: GetFormat) -> Option<ReturnType> {
        match format {
            GetFormat::Dtf => {
                let mut bytes: Vec<u8> = Vec::new();
                let _ = dtf::write_batches(&mut bytes, &result);
                Some(ReturnType::Bytes(bytes))
            }
            GetFormat::Json => {
                Some(ReturnType::String(
                    format!("[{}]\n", dtf::update_vec_to_json(&result)),
                ))
            }
            GetFormat::Csv => {
                Some(ReturnType::String(
                    format!("{}\n", dtf::update_vec_to_csv(&result)),
                ))
            }
        }
    }

    /// create a new store
    pub fn new(global: &Global) -> State {
        let dtf_folder: &str = &global.read().unwrap().settings.dtf_folder;
        let mut state = State {
            current_store_name: "default".to_owned(),
            is_adding: false,
            bulkadd_db: None,
            is_subscribed: false,
            subscribed_db: None,
            sub_id: None,
            rx: None,
            store: HashMap::new(),
            global: global.clone(),
        };

        // insert default first, if there is a copy in memory this will be replaced
        let default_file = format!("{}/default.dtf", dtf_folder);
        let default_in_memory = !Path::new(&default_file).exists();
        state.store.insert(
            "default".to_owned(),
            Store {
                name: "default".to_owned(),
                fname: format!("{}--default", Uuid::new_v4()),
                in_memory: default_in_memory,
                global: global.clone(),
            },
        );

        let rdr = global.read().unwrap();
        for (store_name, _vec) in &rdr.vec_store {
            let fname = format!("{}/{}.dtf", dtf_folder, store_name);
            let in_memory = !Path::new(&fname).exists();
            state.store.insert(
                store_name.to_owned(),
                Store {
                    name: store_name.to_owned(),
                    fname: format!("{}--{}", Uuid::new_v4(), store_name),
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

/// key: btc_neo
///      btc_eth
///      ..
///      total
pub type History = HashMap<String, Vec<(SystemTime, u64)>>;


#[derive(Debug)]
pub struct SharedState {
    pub n_cxns: u16,
    pub settings: Settings,
    pub vec_store: HashMap<String, VecStore>,
    pub history: History,
    pub subs: Arc<Mutex<Subscriptions>>,
}

impl SharedState {
    pub fn new(settings: Settings) -> SharedState {
        let mut hashmap = HashMap::new();
        hashmap.insert("default".to_owned(), (box Vec::new(), 0));
        let subs = Arc::new(Mutex::new(Subscriptions::new()));
        SharedState {
            n_cxns: 0,
            settings,
            vec_store: hashmap,
            history: HashMap::new(),
            subs,
        }
    }
}
