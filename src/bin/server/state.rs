use dtf;
use std::collections::HashMap;
use utils;
use std::path::Path;
use settings::Settings;

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
    pub folder: String,
    pub in_memory: bool,
    pub size: u64,
    pub v: Vec<dtf::Update>
}

impl Store {
    /// Push a new `Update` into the vec
    pub fn add(&mut self, new_vec : dtf::Update) {
        self.size = self.size + 1;
        self.v.push(new_vec);
    }

    /// write items stored in memory into file
    /// If file exists, use append which only appends a filtered set of updates whose timestamp is larger than the old timestamp
    /// If file doesn't exists, simply encode.
    /// 
    /// TODO: Need to figure out how to specify symbol (and exchange name).
    pub fn flush(&self) -> Option<bool> {
        let fname = format!("{}/{}.dtf", self.folder, self.name);
        utils::create_dir_if_not_exist(&self.folder);
        if Path::new(&fname).exists() {
            dtf::append(&fname, &self.v);
            return Some(true);
        } else {
            dtf::encode(&fname, &self.name /*XXX*/, &self.v);
        }
        Some(true)
    }

    /// load items from dtf file
    pub fn load(&mut self) {
        let fname = format!("{}/{}.dtf", self.folder, self.name);
        if Path::new(&fname).exists() && !self.in_memory {
            self.v = dtf::decode(&fname);
            self.size = self.v.len() as u64;
            self.in_memory = true;
        }
    }

    /// load size from file
    pub fn load_size_from_file(&mut self) {
        let header_size = dtf::get_size(&format!("{}/{}.dtf", self.folder, self.name));
        self.size = header_size;
    }

    /// clear the vector. toggle in_memory. update size
    pub fn clear(&mut self) {
        self.v.clear();
        self.in_memory = false;
        self.load_size_from_file();
    }
}


/// Each client gets its own State
pub struct State {
    pub is_adding: bool,
    pub store: HashMap<String, Store>,
    pub current_store_name: String,
    pub settings: Settings
}

impl State {

    pub fn info(&self) -> String {
        let info_vec : Vec<String> = self.store.values().map(|store| {
            format!(r#"{{"name": "{}", "in_memory": {}, "count": {}}}"#, store.name, store.in_memory, store.size)
        }).collect();
        format!("[{}]\n", info_vec.join(", "))
    }

    pub fn insert(&mut self, up: dtf::Update, store_name : &str) -> Option<()> {
        match self.store.get_mut(store_name) {
            Some(store) => {
                store.add(up);
                Some(())
            }
            None => None
        }
    }

    pub fn add(&mut self, up: dtf::Update) {
        let current_store = self.get_current_store();
        current_store.add(up);
    }

    pub fn autoflush(&mut self) {
        let current_store = self.store.get_mut(&self.current_store_name).expect("KEY IS NOT IN HASHMAP");
        if self.settings.autoflush
           && current_store.size % u64::from(self.settings.flush_interval) == 0 {
            println!("(AUTO) FLUSHING!");
            current_store.flush();
            current_store.load_size_from_file();
        }
    }

    pub fn create(&mut self, dbname: &str) {
        let folder = self.get_folder();
        self.store.insert(dbname.to_owned(), Store {
            name: dbname.to_owned(),
            v: Vec::new(),
            size: 0,
            in_memory: false,
            folder
        });
    }

    fn get_folder(&mut self) -> String {
        self.settings.dtf_folder.clone()
    }

    pub fn use_db(&mut self, dbname: &str) -> Option<()> {
        if self.store.contains_key(dbname) {
            self.current_store_name = dbname.to_owned();
            let current_store = self.get_current_store();
            current_store.load();
            Some(())
        } else {
            None
        }
    }

    pub fn get_all_as_json(&mut self) -> String {
        let json = dtf::update_vec_to_json(&self.get_current_store().v);
        format!("[{}]\n", json)
    }

    pub fn get_n_as_json(&mut self, count: i32) -> Option<String> {
        let current_store = self.get_current_store();

        if (current_store.size as i32) <= count || current_store.size == 0 {
            None
        } else {
            let json = dtf::update_vec_to_json(&current_store.v[..count as usize]);
            let json = format!("[{}]\n", json);
            Some(json)
        }
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
        let current_store = self.get_current_store(); 
        if (current_store.size as i32) < count || current_store.size == 0 {
            None
        } else {
            match count {
                -1 => {
                    dtf::write_batches(&mut bytes, &current_store.v);
                },
                _ => {
                    dtf::write_batches(&mut bytes, &current_store.v[..count as usize]);
                }
            }
            Some(bytes)
        }
    }

    pub fn new(settings: &Settings, dtf_folder: &str) -> State {
        let mut state = State {
            current_store_name: "default".to_owned(),
            is_adding: false,
            store: HashMap::new(),
            settings: settings.clone()
        };
        let default_file = format!("{}/default.dtf", settings.dtf_folder);
        let default_in_memory = !Path::new(&default_file).exists();
        state.store.insert("default".to_owned(), Store {
            name: "default".to_owned(),
            v: Vec::new(),
            size: 0,
            in_memory: default_in_memory,
            folder: dtf_folder.to_owned(),
        });
        state
    }
}