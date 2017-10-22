use state::*;
use parser;

static HELP_STR : &str = "PING, INFO, USE [db], CREATE [db],
ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
BULKADD ...; DDAKLUB
FLUSH, FLUSHALL, GETALL, GET [count], CLEAR
";

pub type Response = (Option<String>, Option<Vec<u8>>, Option<String>);

pub fn gen_response(string : &str, state: &mut State) -> Response {
    match string {
        "" => (Some("\n".to_owned()), None, None),
        "PING" => (Some("PONG.\n".to_owned()), None, None),
        "HELP" => (Some(HELP_STR.to_owned()), None, None),
        "INFO" => (Some(state.info()), None, None),
        "BULKADD" => {
            state.is_adding = true;
            (Some("\n".to_owned()), None, None)
        },
        "DDAKLUB" => {
            state.is_adding = false;
            state.bulkadd_db = None;
            (Some("1\n".to_owned()), None, None)
        },
        "GET ALL" =>  {
            match state.get(-1) {
                Some(bytes) => (None, Some(bytes), None),
                None => (None, None, Some("Failed to GET ALL.".to_owned()))
            }
        },
        "GET ALL AS JSON" => (Some(state.get_all_as_json()), None, None),
        "CLEAR" => { state.clear(); (Some("1\n".to_owned()), None, None) },
        "CLEAR ALL" => {
            state.clearall();
            (Some("1\n".to_owned()), None, None)
        },
        "FLUSH" => {
            state.flush();
            (Some("1\n".to_owned()), None, None)
        },
        "FLUSH ALL" => {
            state.flushall();
            (Some("1\n".to_owned()), None, None)
        },
        _ => {
            // is in bulkadd
            if state.is_adding {
                let parsed = parser::parse_line(string);
                match parsed {
                    Some(up) => {
                        let current_db = state.bulkadd_db.clone();
                        match current_db {
                            Some(ref dbname) => {
                                state.insert(up, &dbname);
                            },
                            None => {
                                state.add(up);
                            }
                        };
                        state.autoflush();
                        (Some("\n".to_owned()), None, None)
                    },
                    None => return (None, None, Some("Unable to parse line in BULKADD".to_owned()))
                }
            } else

            if string.starts_with("BULKADD INTO ") {
                let (_index, dbname) = parser::parse_dbname(string);
                state.bulkadd_db = Some(dbname.to_owned());
                state.is_adding = true;
                (Some("\n".to_owned()), None, None)
            } else 

            if string.starts_with("ADD ") {
                let parsed = if string.contains(" INTO ") {
                    parser::parse_add_into(&string)
                } else {
                    let data_string : &str = &string[3..];
                    match parser::parse_line(&data_string) {
                        Some(up) => Some((up, state.current_store_name.to_owned())),
                        None => None
                    }
                };

                match parsed {
                    Some((up, dbname)) => {
                        match state.insert(up, &dbname) {
                            Some(()) => {
                                state.autoflush();
                                (Some("1\n".to_owned()), None, None)
                            }
                            None => {
                                (None, None, Some(format!("db `{}` not found", dbname)))
                            }
                        }
                    },
                    None => return (None, None, Some("parsing ADD INTO".to_owned()))
                }
            } else 

            if string.starts_with("CREATE ") {
                let dbname : &str = &string[7..];
                state.create(dbname);
                (Some(format!("Created DB `{}`.\n", &dbname)), None, None)
            } else

            if string.starts_with("USE ") {
                let dbname : &str = &string[4..];
                match state.use_db(dbname) {
                    Some(_) => (Some(format!("SWITCHED TO DB `{}`.\n", &dbname)), None, None),
                    None => (None, None, Some(format!("No db named `{}`", dbname)))
                }
            } else

            // get
            if string.starts_with("GET ") {
                let count : &str = &string[4..];
                let count : Vec<&str> = count.split(" ").collect();
                let count = count[0].parse::<i32>().unwrap();
                if string.contains("AS JSON") {
                    match state.get_n_as_json(count) {
                        Some(json) => (Some(json), None, None),
                        None => (None, None, Some(format!("Requested {} items. Too many.", count)))
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