use state::*;
use parser;
use libtectonic::dtf::{update_vec_to_json, Update};

// BUG: subscribe, add, deadlock!!!

#[derive(Debug, PartialEq, Eq)]
pub enum ReturnType {
    String(String),
    Bytes(Vec<u8>),
    Error(String),
}

#[derive(Debug)]
pub enum ReqCount {
    All,
    Count(u32),
}

#[derive(Debug)]
pub enum GetFormat {
    Json,
    Csv,
    Dtf,
}

type DbName = String;

#[derive(Debug)]
pub enum Loc {
    Mem,
    Fs,
}

pub type Range = Option<(u64, u64)>;

#[derive(Debug)]
enum Command {
    Nothing,
    Ping,
    Help,
    Info,
    Perf,
    BulkAdd,
    BulkAddInto(DbName),
    BulkAddEnd,
    Get(ReqCount, GetFormat, Range, Loc),
    Count(ReqCount, Loc),
    Clear(ReqCount),
    Flush(ReqCount),
    Insert(Option<Update>, Option<DbName>),
    Create(DbName),
    Subscribe(DbName),
    Unsubscribe(ReqCount),
    Subscription,
    Use(DbName),
    Exists(DbName),
    Unknown,
}

static HELP_STR: &str = "PING, INFO, USE [db], CREATE [db],
ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
BULKADD ...; DDAKLUB
FLUSH, FLUSHALL, GETALL, GET [count], CLEAR
";

/// sometimes returns string, sometimes bytes, error string
// pub type Response = (Option<String>, Option<Vec<u8>>, Option<String>);

pub fn gen_response(string: &str, state: &mut State) -> ReturnType {
    use self::Command::*;

    let command: Command = match string {
        "" => {
            if state.is_subscribed {
                Subscription
            } else {
                Nothing
            }
        }
        "PING" => Ping,
        "HELP" => Help,
        "INFO" => Info,
        "PERF" => Perf,
        "BULKADD" => BulkAdd,
        "DDAKLUB" => BulkAddEnd,
        "UNSUBSCRIBE" => Unsubscribe(ReqCount::Count(0)),
        "UNSUBSCRIBE ALL" => Unsubscribe(ReqCount::All),
        "COUNT" => Count(ReqCount::Count(1), Loc::Fs), 
        "COUNT ALL" => Count(ReqCount::All, Loc::Fs),
        "COUNT ALL IN MEM" => Count(ReqCount::All, Loc::Mem),
        "CLEAR" => Clear(ReqCount::Count(1)),
        "CLEAR ALL" => Clear(ReqCount::All),
        "GET ALL AS JSON" => Get(ReqCount::All, GetFormat::Json, None, Loc::Mem),
        "GET ALL AS CSV" => Get(ReqCount::All, GetFormat::Csv, None, Loc::Mem),
        "GET ALL" => Get(ReqCount::All, GetFormat::Dtf, None, Loc::Mem),
        "FLUSH" => Flush(ReqCount::Count(1)),
        "FLUSH ALL" => Flush(ReqCount::All),
        _ => {
            // is in bulkadd
            if state.is_adding {
                let parsed = parser::parse_line(string);
                let current_db = state.bulkadd_db.clone();
                let dbname = current_db.unwrap();
                Insert(parsed, Some(dbname))
            } else if string.starts_with("BULKADD INTO ") {
                let (_index, dbname) = parser::parse_dbname(string);
                BulkAddInto(dbname.to_owned())
            } else if string.starts_with("SUBSCRIBE ") {
                let dbname: &str = &string[10..];
                Subscribe(dbname.to_owned())
            } else if string.starts_with("CREATE ") {
                let dbname: &str = &string[7..];
                Create(dbname.to_owned())
            } else if string.starts_with("USE ") {
                let dbname: &str = &string[4..];
                Use(dbname.to_owned())
            } else if string.starts_with("EXISTS ") {
                let dbname: &str = &string[7..];
                Exists(dbname.to_owned())
            } else if string.starts_with("ADD ") || string.starts_with("INSERT ") {
                let parsed = if string.contains(" INTO ") {
                    parser::parse_add_into(&string)
                } else {
                    let data_string: &str = &string[3..];
                    match parser::parse_line(&data_string) {
                        Some(up) => (Some(up), Some(state.current_store_name.to_owned())),
                        None => (None, None),
                    }
                };
                Insert(parsed.0, parsed.1)
            } else
            // get
            if string.starts_with("GET ") {
                // how many records we want...
                let count = if string.starts_with("GET ALL ") {
                    ReqCount::All
                } else {
                    let count: &str = &string.clone()[4..];
                    let count: Vec<&str> = count.split(" ").collect();
                    let count = count[0].parse::<u32>().unwrap_or(1);
                    ReqCount::Count(count)
                };

                let range = parser::parse_get_range(string);

                // test if is Json
                let format = if string.contains(" AS JSON") {
                    GetFormat::Json
                } else {
                    if string.contains(" AS CSV") { GetFormat::Csv }
                    else { GetFormat::Dtf }
                };
                let loc = if string.contains(" IN MEM") { Loc::Mem } else { Loc::Fs };

                Get(count, format, range, loc)
            } else {
                Unknown
            }
        }
    };

    match command {
        Nothing => return_string(""),
        Ping => return_string("PONG"),
        Help => return_string(HELP_STR),
        Info => return_string(&state.info()),
        Perf => return_string(&state.perf()),
        BulkAdd => {
            state.is_adding = true;
            return_string("")
        }
        BulkAddInto(dbname) => {
            state.bulkadd_db = Some(dbname);
            state.is_adding = true;
            return_string("")
        }
        BulkAddEnd => {
            state.is_adding = false;
            state.bulkadd_db = None;
            return_string("1")
        }
        Count(ReqCount::Count(_), Loc::Fs) => return_string(&format!("{}", state.count())),
        Count(ReqCount::Count(_), Loc::Mem) => return_string(&format!("{}", state.count())), // TODO: implement count in mem
        Count(ReqCount::All, Loc::Fs) => return_string(&format!("{}", state.countall())),
        Count(ReqCount::All, Loc::Mem) => return_string(&format!("{}", state.countall_in_mem())),
        Clear(ReqCount::Count(_)) => {
            state.clear();
            return_string("1")
        }
        Clear(ReqCount::All) => {
            state.clearall();
            return_string("1")
        }
        Flush(ReqCount::Count(_)) => {
            state.flush();
            return_string("1")
        }
        Flush(ReqCount::All) => {
            state.flushall();
            return_string("1")
        }

        // update, dbname
        Insert(Some(up), Some(dbname)) => {
            match state.insert(up, &dbname) {
                Some(()) => return_string(""),
                None => return_err(&format!("DB {} not found.", dbname)),
            }
        }
        Insert(Some(up), None) => {
            state.add(up);
            return_string("")
        }
        Insert(None, _) => return_err("Unable to parse line"),

        Create(dbname) => {
            state.create(&dbname);
            return_string(&format!("Created DB `{}`.", &dbname))
        }

        Subscribe(dbname) => {
            state.sub(&dbname);
            return_string(&format!("Subscribed to {}", dbname))
        }

        Subscription => {
            let rxlocked = state.rx.clone().unwrap();
            let message = rxlocked.lock().unwrap().try_recv();
            match message {
                Ok(msg) => return_string(&update_vec_to_json(&vec![msg])),
                _ => return_string("NONE"),
            }
        }

        Unsubscribe(ReqCount::All) => {
            state.unsub_all();
            return_string("Unsubscribed everything!")
        }

        Unsubscribe(ReqCount::Count(_)) => {
            let old_dbname = state.subscribed_db.clone().unwrap();
            state.unsub();
            return_string(&format!("Unsubscribed from {}", old_dbname))
        }

        Use(dbname) => {
            match state.use_db(&dbname) {
                Some(_) => return_string(&format!("SWITCHED TO DB `{}`.", &dbname)),
                None => return_err(&format!("No db named `{}`", dbname)),
            }
        }
        Exists(dbname) => {
            if state.exists(&dbname) {
                return_string("1")
            } else {
                return_err(&format!("No db named `{}`", dbname))
            }
        }

        // get
        Get(cnt, fmt, rng, loc) => {
            match state.get(cnt, fmt, rng, loc) {
                Some(ReturnType::Bytes(b)) => return_bytes(b),
                Some(ReturnType::String(s)) => return_string(&s),
                Some(ReturnType::Error(e)) => return_err(&e),
                None => return_err("Not enough items to return."),
            }
        }

        Unknown => return_err("Unknown command."),
    }
}

fn return_string(string: &str) -> ReturnType {
    let mut ret = String::new();
    ret.push_str(string);
    ret.push_str("\n");
    ReturnType::String(ret)
}

fn return_bytes(bytes: Vec<u8>) -> ReturnType {
    ReturnType::Bytes(bytes)
}

fn return_err(err: &str) -> ReturnType {
    let mut ret = String::new();
    ret.push_str(err);
    ret.push_str("\n");
    ReturnType::Error(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use settings::Settings;
    use std::sync::{Arc, RwLock};

    fn gen_state() -> State {
        let settings: Settings = Default::default();
        let global = Arc::new(RwLock::new(SharedState::new(settings)));
        State::new(&global)
    }

    #[test]
    fn should_return_pong() {
        let mut state = gen_state();
        let resp = gen_response("PING", &mut state);
        assert_eq!(ReturnType::String(String::from("PONG\n")), resp);
    }

    #[test]
    fn should_not_insert_into_empty() {
        let mut state = gen_state();
        let resp = gen_response(
            "ADD 1513749530.585,0,t,t,0.04683200,0.18900000; INTO bnc_btc_eth",
            &mut state,
        );
        assert_eq!(
            ReturnType::Error(String::from("DB bnc_btc_eth not found.\n")),
            resp
        );
    }

    #[test]
    fn should_insert_ok() {
        let mut state = gen_state();
        let resp = gen_response("CREATE bnc_btc_eth", &mut state);
        assert_eq!(
            ReturnType::String(String::from("Created DB `bnc_btc_eth`.\n")),
            resp
        );
        let resp = gen_response(
            "ADD 1513749530.585,0,t,t,0.04683200,0.18900000; INTO bnc_btc_eth",
            &mut state,
        );
        assert_eq!(ReturnType::String(String::from("\n")), resp);
    }

}
