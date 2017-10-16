use state::*;
use parser;
use dtf;

static HELP_STR : &str = "PING, INFO, USE [db], CREATE [db],
ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
BULKADD ...; DDAKLUB
FLUSH, FLUSHALL, GETALL, GET [count], CLEAR
";

pub fn gen_response(string : &str, state: &mut State) -> (Option<String>, Option<Vec<u8>>, Option<String>) {
    match string {
        "" => (Some("".to_owned()), None, None),
        "PING" => (Some("PONG.\n".to_owned()), None, None),
        "HELP" => (Some(HELP_STR.to_owned()), None, None),
        "INFO" => {
            let info_vec : Vec<String> = state.store.values().map(|store| {
                format!(r#"{{"name": "{}", "in_memory": {}, "count": {}}}"#, store.name, store.in_memory, store.size)
            }).collect();

            (Some(format!("[{}]\n", info_vec.join(", "))), None, None)
        },
        "BULKADD" => {
            state.is_adding = true;
            (Some("".to_owned()), None, None)
        },
        "DDAKLUB" => {
            state.is_adding = false;
            (Some("1\n".to_owned()), None, None)
        },
        "GET ALL AS JSON" => {
            let current_store = state.store.get(&state.current_store_name).unwrap();
            let json = dtf::update_vec_to_json(&current_store.v);
            let json = format!("[{}]\n", json);
            (Some(json), None, None)
        },
        "GET ALL" => {
            match state.get(-1) {
                Some(bytes) => (None, Some(bytes), None),
                None => (None, None, Some("Failed to GET ALL.".to_owned()))
            }
        },
        "CLEAR" => {
            let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
            current_store.clear();
            (Some("1\n".to_owned()), None, None)
        },
        "CLEAR ALL" => {
            for store in state.store.values_mut() {
                store.clear();
            }
            (Some("1\n".to_owned()), None, None)
        },
        "FLUSH" => {
            let current_store = state.store.get_mut(&state.current_store_name).expect("KEY IS NOT IN HASHMAP");
            current_store.flush();
            (Some("1\n".to_owned()), None, None)
        },
        "FLUSH ALL" => {
            for store in state.store.values() {
                store.flush();
            }
            (Some("1\n".to_owned()), None, None)
        },
        _ => {
            // bulkadd and add
            if state.is_adding {
                let parsed = parser::parse_line(string);
                match parsed {
                    Some(up) => {
                        state.add(up);
                        state.autoflush();
                    }
                    None => return (None, None, Some("Unable to parse line in BULKALL".to_owned()))
                }
                (Some("".to_owned()), None, None)
            } else

            if string.starts_with("ADD ") {
                if string.contains(" INTO ") {
                    let into_indices : Vec<_> = string.match_indices(" INTO ").collect();
                    let (index, _) = into_indices[0];
                    let dbname = &string[(index+6)..];
                    let data_string : &str = &string[3..(index)];
                    match parser::parse_line(&data_string) {
                        Some(up) => {
                            match state.insert(up, dbname) {
                                Some(true) => {
                                    state.autoflush();
                                    (Some("1\n".to_owned()), None, None)
                                }
                                _ => {
                                    (None, None, Some(format!("db `{}` not found", dbname)))
                                }
                            }
                        },
                        None => return (None, None, Some("parsing ADD INTO".to_owned()))
                    }
                } else {
                    let data_string : &str = &string[3..];
                    match parser::parse_line(&data_string) {
                        Some(up) => {
                            state.add(up);
                            state.autoflush();
                            (Some("1\n".to_owned()), None, None)
                        }
                        None => return (None, None, Some("Parse ADD".to_owned()))
                    }
                }
            } else 

            // db commands
            if string.starts_with("CREATE ") {
                let dbname : &str = &string[7..];
                state.store.insert(dbname.to_owned(), Store {
                    name: dbname.to_owned(),
                    v: Vec::new(),
                    size: 0,
                    in_memory: false,
                    folder: state.settings.dtf_folder.clone()
                });
                (Some(format!("Created DB `{}`.\n", &dbname)), None, None)
            } else

            if string.starts_with("USE ") {
                let dbname : &str = &string[4..];
                if state.store.contains_key(dbname) {
                    state.current_store_name = dbname.to_owned();
                    let current_store = state.store.get_mut(&state.current_store_name).unwrap();
                    current_store.load();
                    (Some(format!("SWITCHED TO DB `{}`.\n", &dbname)), None, None)
                } else {
                    (None, None, Some(format!("State does not contain {}", dbname)))
                }
            } else

            // get
            if string.starts_with("GET ") {
                let num : &str = &string[4..];
                let count : Vec<&str> = num.split(" ").collect();
                let count = count[0].parse::<i32>().unwrap();

                if string.contains("AS JSON") {
                    let current_store = state.store.get(&state.current_store_name).unwrap();

                    if (current_store.size as i32) <= count || current_store.size == 0 {
                        (None, None, Some("Requested too many".to_owned()))
                    } else {
                        let json = dtf::update_vec_to_json(&current_store.v[..count as usize]);
                        let json = format!("[{}]\n", json);
                        (Some(json), None, None)
                    }
                } else {
                    match state.get(count) {
                        Some(bytes) => (None, Some(bytes), None),
                        None => (None, None, Some(format!("Failed to get {}.", count)))
                    }
                }
            }

            else {
                (None, None, Some("Unsupported command.".to_owned()))
            }
        }
    }
}