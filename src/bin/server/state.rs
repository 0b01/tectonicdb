use dtf;
use std::collections::HashMap;
use utils;
use std::path::Path;
use settings::Settings;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub in_memory: bool,
    pub global: Global
}

/// An atomic reference counter for accessing shared data.
pub type Global = Arc<RwLock<SharedState>>;

impl Store {
    /// Push a new `Update` into the vec
    pub fn add(&mut self, new_vec : dtf::Update) {
        let mut wtr = self.global.write().unwrap();
        let vecs = wtr.vec_store.get_mut(&self.name).expect("KEY IS NOT IN HASHMAP");

        vecs.0.push(new_vec);
        vecs.1 += 1;
    }

    pub fn count(&self) -> u64 {
        let rdr = self.global.read().unwrap();
        let vecs = rdr.vec_store.get(&self.name).expect("KEY IS NOT IN HASHMAP");
        vecs.1
    }

    /// write items stored in memory into file
    /// If file exists, use append which only appends a filtered set of updates whose timestamp is larger than the old timestamp
    /// If file doesn't exists, simply encode.
    ///
    /// TODO: Need to figure out how to specify symbol (and exchange name).
    pub fn flush(&self) -> Option<bool> {
        let folder = self.global.read().unwrap().settings.dtf_folder.to_owned();
        let fname = format!("{}/{}.dtf", &folder, self.name);
        utils::create_dir_if_not_exist(&folder);
        if Path::new(&fname).exists() {
            let rdr = self.global.read().unwrap();
            let vecs = rdr.vec_store.get(&self.name).expect("KEY IS NOT IN HASHMAP");
            dtf::append(&fname, &vecs.0);
            return Some(true);
        } else {
            let rdr = self.global.read().unwrap();
            let vecs = rdr.vec_store.get(&self.name).expect("KEY IS NOT IN HASHMAP");
            dtf::encode(&fname, &self.name /*XXX*/, &vecs.0);
        }
        Some(true)
    }

    /// load items from dtf file
    fn load(&mut self) {
        let folder = self.global.read().unwrap().settings.dtf_folder.to_owned();
        let fname = format!("{}/{}.dtf", &folder, self.name);
        if Path::new(&fname).exists() && !self.in_memory {
            let ups = dtf::decode(&fname);
            let mut wtr = self.global.write().unwrap();
            let size = ups.len() as u64;
            wtr.vec_store.insert(self.name.to_owned(), (ups, size));
            self.in_memory = true;
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

        let mut wtr = self.global.write().unwrap();
        wtr.vec_store
            .get_mut(&self.name)
            .expect("Key is not in vec_store")
            .1 = header_size;
    }

    /// clear the vector. toggle in_memory. update size
    pub fn clear(&mut self) {
        {
            let mut rdr = self.global.write().unwrap();
            let vecs = (*rdr).vec_store.get_mut(&self.name).expect("KEY IS NOT IN HASHMAP");
            vecs.0.clear();
            vecs.1 = 0;
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

    /// mapping store_name -> Store
    pub store: HashMap<String, Store>,

    /// the current STORE client is using
    pub current_store_name: String,

    /// shared data
    pub global: Global
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
        let info_vec : Vec<String> = rdr.vec_store.iter().map(|i| {
            let (key, value) = i;
            let vecs = &value.0;
            let size = value.1;
            format!(r#"{{
    "name": "{}",
    "in_memory": {},
    "count": {}
  }}"#,
                        key,
                        !vecs.is_empty(),
                        size
                   )
        }).collect();


        let metadata = format!(r#"{{
    "cxns": {},
    "max_threads": {},
    "ts": {},
    "autoflush_enabled": {},
    "autoflush_interval": {},
    "dtf_folder": "{}",
    "total_count": {}
  }}"#,

                rdr.connections,
                rdr.settings.threads,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                rdr.settings.autoflush,
                rdr.settings.flush_interval,
                rdr.settings.dtf_folder,
                rdr.vec_store.iter().fold(0, |acc, (_name, tup)| acc + tup.1)
            );
        let mut ret = format!(r#"{{
  "meta": {},
  "dbs": [{}]
}}"#,
            metadata,
            info_vec.join(", "));
        ret.push('\n');
        ret
    }

    pub fn perf(&self) -> String {
        let rdr = self.global.read().unwrap();
        let size_t = &rdr.history;
        format!("{:?}\n", size_t)
    }

    /// Insert a row into store
    pub fn insert(&mut self, up: dtf::Update, store_name : &str) -> Option<()> {
        match self.store.get_mut(store_name) {
            Some(store) => {
                store.add(up);
                Some(())
            }
            None => None
        }
    }

    /// Insert a row into current store.
    pub fn add(&mut self, up: dtf::Update) {
        let current_store = self.get_current_store();
        current_store.add(up);
    }

    /// Saves current store into disk after n items is inserted.
    pub fn autoflush(&mut self) {
        let shared_state = self.global.read().unwrap();
        let is_autoflush = shared_state.settings.autoflush;
        let flush_interval = shared_state.settings.flush_interval;
        let sizes : Vec<(&String, u64)> = shared_state.vec_store
                                            .iter()
                                            .map(|(k, v)| (k, v.1))
                                            .collect();
        for (name, size) in sizes {
            if is_autoflush
                && size != 0
                && size % u64::from(flush_interval) == 0 {

                let st = self.store
                            .get_mut(name)
                            .expect("KEY IS NOT IN HASHMAP");

                debug!("AUTOFLUSHING!");
                st.flush();
                st.load_size_from_file();
            }
        }

    }

    /// Create a new store
    pub fn create(&mut self, store_name: &str) {
        // insert a vector into shared hashmap
        {
            let mut global = self.global.write().unwrap();
            global.vec_store.insert(store_name.to_owned(), (Vec::new(), 0));
        }
        // insert a store into client state hashmap
        self.store.insert(store_name.to_owned(), Store {
            name: store_name.to_owned(),
            in_memory: false,
            global: self.global.clone()
        });
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

    /// returns every row formatted as JSON
    pub fn get_all_as_json(&mut self) -> String {
        let shared_state = self.global.read().unwrap();
        let vecs = &shared_state.vec_store
                    .get(&self.current_store_name)
                    .expect("Key is not in vec_store")
                    .0;
        let json = dtf::update_vec_to_json(vecs);
        format!("[{}]\n", json)
    }

    pub fn count(&mut self) -> u64 {
        let store = self.get_current_store();
        store.count() 
    }

    pub fn countall(&self) -> u64 {
        let rdr = self.global.read().unwrap();
        rdr.vec_store.iter().fold(0, |acc, (_name, tup)| acc + tup.1)
    }

    pub fn get_n_as_json(&mut self, count: i32) -> Option<String> {
        {
            let shared_state = self.global.read().unwrap();
            let size = shared_state.vec_store
                        .get(&self.current_store_name)
                        .expect("Key is not in vec_store")
                        .1;

            if (size as i32) < count || size == 0 {
                return None
            }
        }

        let shared_state = self.global.read().unwrap();
        let vecs = &shared_state.vec_store
                    .get(&self.current_store_name)
                    .expect("Key is not in vec_store")
                    .0;
        let json = dtf::update_vec_to_json(&vecs[..count as usize]);
        let json = format!("[{}]\n", json);
        Some(json)
    }

    pub fn clear(&mut self) {
        self.get_current_store().clear();
    }

    pub fn clearall(&mut self) {
        for store in self.store.values_mut() {
            store.clear();
        }
    }

    pub fn flush(&mut self) {
        self.get_current_store().flush();
    }

    pub fn flushall(&mut self) {
        for store in self.store.values() {
            store.flush();
        }
    }

    fn get_current_store(&mut self) -> &mut Store {
        self.store.get_mut(&self.current_store_name).expect("KEY IS NOT IN HASHMAP")
    }

    pub fn get(&mut self, count : i32) -> Option<Vec<u8>> {
        let mut bytes : Vec<u8> = Vec::new();
        {
            let shared_state = self.global.read().unwrap();
            let size = shared_state.vec_store
                        .get(&self.current_store_name)
                        .expect("Key is not in vec_store")
                        .1;

            if (size as i32) < count || size == 0 {
                return None
            }
        }

        let rdr = self.global.read().unwrap();
        let vecs = rdr.vec_store.get(&self.current_store_name).expect("KEY IS NOT IN HASHMAP");
        match count {
            -1 => {
                dtf::write_batches(&mut bytes, &vecs.0);
            },
            _ => {
                dtf::write_batches(&mut bytes, &vecs.0[..count as usize]);
            }
        }
        Some(bytes)
    }

    pub fn new(global: &Global) -> State {
        let dtf_folder: &str = &global.read().unwrap().settings.dtf_folder;
        let mut state = State {
            current_store_name: "default".to_owned(),
            bulkadd_db: None,
            is_adding: false,
            store: HashMap::new(),
            global: global.clone()
        };

        // insert default first, if there is a copy in memory this will be replaced
        let default_file = format!("{}/default.dtf", dtf_folder);
        let default_in_memory = !Path::new(&default_file).exists();
        state.store.insert("default".to_owned(), Store {
            name: "default".to_owned(),
            in_memory: default_in_memory,
            global: global.clone()
        });

        let rdr = global.read().unwrap();
        for (store_name, _vec) in &rdr.vec_store {
            let fname = format!("{}/{}.dtf", dtf_folder, store_name);
            let in_memory = !Path::new(&fname).exists();
            state.store.insert(store_name.to_owned(), Store {
                name: store_name.to_owned(),
                in_memory: in_memory,
                global: global.clone()
            });
        }
        state
    }
}

/// (updates, count)
pub type VecStore = (Vec<dtf::Update>, u64);

/// (time, total_count, name to size)
pub type History= (SystemTime, u64, HashMap<String, u64>);

#[derive(Debug)]
pub struct SharedState {
    pub connections: u16,
    pub settings: Settings,
    pub vec_store: HashMap<String, VecStore>,
    pub history: Vec<History>,
}

impl SharedState {
    pub fn new(settings: Settings) -> SharedState {
        let mut hashmap = HashMap::new();
        hashmap.insert("default".to_owned(), (Vec::new(),0) );
        SharedState {
            connections: 0,
            settings,
            vec_store: hashmap,
            history: Vec::new(),
        }
    }
}
