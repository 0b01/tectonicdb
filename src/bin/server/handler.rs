use crate::prelude::*;

use crate::parser;
use libtectonic::dtf::update::{UpdateVecConvert, Update};
use std::borrow::{Cow, Borrow};

// BUG: subscribe, add, deadlock!!!

#[derive(Debug, PartialEq, Eq)]
pub enum ReturnType<'thread> {
    String(Cow<'thread, str>),
    Bytes(Vec<u8>),
    Error(Cow<'thread, str>),
}

impl<'thread> ReturnType<'thread> {
    pub fn string<S>(string: S) -> ReturnType<'thread>
        where S: Into<Cow<'thread, str>>
    {
        ReturnType::String(string.into())
    }

    pub fn error<S>(string: S) -> ReturnType<'thread>
        where S: Into<Cow<'thread, str>>
    {
        ReturnType::Error(string.into())
    }
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

#[derive(Debug)]
pub enum Loc {
    Mem,
    Fs,
}

pub type Range = Option<(u64, u64)>;

#[derive(Debug)]
pub enum Void {}

#[derive(Debug)]
pub enum Command {
    Noop,
    Ping,
    Help,
    Info,
    Perf,
    Get(ReqCount, GetFormat, Range, Loc),
    Count(ReqCount, Loc),
    Clear(ReqCount),
    Flush(ReqCount),
    AutoFlush(bool),
    Insert(Option<Update>, Option<String>),
    Create(String),
    Subscribe(String),
    Unsubscribe(ReqCount),
    Subscription,
    Use(String),
    Exists(String),
    Unknown,
}

#[derive(Debug)]
pub enum Event {
    NewPeer {
        name: SocketAddr,
        stream: Arc<TcpStream>,
        shutdown: Receiver<Void>,
    },
    TestMessage {
        from: SocketAddr,
    },
    Command {
        from: SocketAddr,
        command: Command
    },
}

static HELP_STR: &str = "PING, INFO, USE [db], CREATE [db],
ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
FLUSH, FLUSH ALL, GET ALL, GET [count], CLEAR
";

/// sometimes returns string, sometimes bytes, error string
// pub type Response = (Option<String>, Option<Vec<u8>>, Option<String>);

pub fn parse_to_event(line: &str) -> Command {
    use self::Command::*;

    match line.borrow() {
        "" => Noop,
        "PING" => Ping,
        "HELP" => Help,
        "INFO" => Info,
        "PERF" => Perf,
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
        "AUTOFLUSH ON" => AutoFlush(true),
        "AUTOFLUSH Off" => AutoFlush(false),
        _ => {
            if line.starts_with("SUBSCRIBE ") {
                let dbname: &str = &line[10..];
                Subscribe(dbname.into())
            } else if line.starts_with("CREATE ") {
                let dbname: &str = &line[7..];
                Create(dbname.into())
            } else if line.starts_with("USE ") {
                let dbname: &str = &line[4..];
                Use(dbname.into())
            } else if line.starts_with("EXISTS ") {
                let dbname: &str = &line[7..];
                Exists(dbname.into())
            } else if line.starts_with("ADD ") || line.starts_with("INSERT ") {
                let (up, dbname) = if line.contains(" INTO ") {
                    let (up, dbname) = parser::parse_add_into(&line);
                    (up, dbname.map(|a| a.into()))
                } else {
                    let data_string: &str = &line[3..];
                    match parser::parse_line(&data_string) {
                        Some(up) => (Some(up), None),
                        None => (None, None),
                    }
                };
                Insert(up, dbname)
            } else
            // get
            if line.starts_with("GET ") {
                // how many records we want...
                let count = if line.starts_with("GET ALL ") {
                    ReqCount::All
                } else {
                    let count: &str = &line.clone()[4..];
                    let count: Vec<&str> = count.split(" ").collect();
                    let count = count[0].parse::<u32>().unwrap_or(1);
                    ReqCount::Count(count)
                };

                let range = parser::parse_get_range(&line);

                // test if is Json
                let format = if line.contains(" AS JSON") {
                    GetFormat::Json
                } else {
                    if line.contains(" AS CSV") { GetFormat::Csv }
                    else { GetFormat::Dtf }
                };
                let loc = if line.contains(" IN MEM") { Loc::Mem } else { Loc::Fs };

                Get(count, format, range, loc)
            } else {
                Unknown
            }
        }
    }
}



// pub fn gen_response<'a: 'b, 'b, 'c>(line: &'b str,
//         state: &'b mut ThreadState<'a, 'c>) -> ReturnType<'a>
//     {
//     match command {
//         Noop => ReturnType::string(""),
//         Ping => ReturnType::string("PONG"),
//         Help => ReturnType::string(HELP_STR),
//         Info => ReturnType::string(state.info()),
//         Perf => ReturnType::string(state.perf()),
//         Count(ReqCount::Count(_), Loc::Fs) => ReturnType::string(format!("{}", state.count())),
//         Count(ReqCount::Count(_), Loc::Mem) => ReturnType::string(format!("{}", state.count_in_mem())),
//         Count(ReqCount::All, Loc::Fs) => ReturnType::string(format!("{}", state.countall())),
//         Count(ReqCount::All, Loc::Mem) => ReturnType::string(format!("{}", state.countall_in_mem())),
//         Clear(ReqCount::Count(_)) => {
//             state.clear();
//             ReturnType::string("1")
//         }
//         Clear(ReqCount::All) => {
//             state.clearall();
//             ReturnType::string("1")
//         }
//         Flush(ReqCount::Count(_)) => {
//             state.flush();
//             ReturnType::string("1")
//         }
//         Flush(ReqCount::All) => {
//             state.flushall();
//             ReturnType::string("1")
//         }

//         AutoFlush(is_autoflush) =>  {
//             state.set_autoflush(is_autoflush);
//             ReturnType::string("1")
//         }

//         // update, dbname
//         Insert(Some(up), Some(dbname)) => {
//             match state.insert(up, &dbname) {
//                 Some(()) => ReturnType::string(""),
//                 None => ReturnType::error(format!("DB {} not found.", dbname)),
//             }
//         }
//         Insert(Some(up), None) => {
//             state.add(up);
//             ReturnType::string("")
//         }
//         Insert(None, _) => ReturnType::error("Unable to parse line"),

//         Create(dbname) => {
//             state.create(&dbname);
//             ReturnType::string(format!("Created DB `{}`.", &dbname))
//         }

//         Subscribe(dbname) => {
//             state.sub(&dbname);
//             ReturnType::string(format!("Subscribed to {}", dbname))
//         }

//         Subscription => {
//             let message = state.rx.as_ref().unwrap().try_recv();
//             match message {
//                 Ok(msg) => ReturnType::string([msg].as_json()),
//                 _ => ReturnType::string("NONE"),
//             }
//         }

//         Unsubscribe(ReqCount::All) => {
//             state.unsub_all();
//             ReturnType::string("Unsubscribed everything!")
//         }

//         Unsubscribe(ReqCount::Count(_)) => {
//             let old_dbname = state.subscribed_db.clone().unwrap();
//             state.unsub();
//             ReturnType::string(format!("Unsubscribed from {}", old_dbname))
//         }

//         Use(dbname) => {
//             match state.use_db(&dbname) {
//                 Some(_) => ReturnType::string(format!("SWITCHED TO DB `{}`.", &dbname)),
//                 None => ReturnType::error(format!("No db named `{}`", dbname)),
//             }
//         }
//         Exists(dbname) => {
//             if state.exists(&dbname) {
//                 ReturnType::string("1")
//             } else {
//                 ReturnType::error(format!("No db named `{}`", dbname))
//             }
//         }

//         Get(cnt, fmt, rng, loc) =>
//             state.get(cnt, fmt, rng, loc)
//             .unwrap_or(ReturnType::error("Not enough items to return")),

//         Unknown => ReturnType::error("Unknown command."),
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::Settings;
    use std::sync::{Arc, RwLock};
    use std::collections::HashMap;
    use futures;

    fn gen_state<'thr, 'store>() -> ThreadState<'thr, 'store> {
        let settings: Settings = Default::default();
        let global = Arc::new(RwLock::new(SharedState::new(settings)));
        let store = Arc::new(RwLock::new(HashMap::new()));
        let (tx, _) = mpsc::unbounded::<Update>();
        ThreadState::new(global, store, tx)
    }

    #[test]
    fn should_return_pong() {
        let mut state = gen_state();
        let resp = gen_response("PING", &mut state);
        assert_eq!(ReturnType::String("PONG".into()), resp);
    }

    #[test]
    fn should_not_insert_into_empty() {
        let mut state = gen_state();
        let resp = gen_response(
            "ADD 1513749530.585,0,t,t,0.04683200,0.18900000; INTO bnc_btc_eth",
            &mut state,
        );
        assert_eq!(
            ReturnType::Error("DB bnc_btc_eth not found.".into()),
            resp
        );
    }

    #[test]
    fn should_insert_ok() {
        let mut state = gen_state();
        let resp = gen_response("CREATE bnc_btc_eth", &mut state);
        assert_eq!(
            ReturnType::String("Created DB `bnc_btc_eth`.".into()),
            resp
        );
        let resp = gen_response(
            "ADD 1513749530.585,0,t,t,0.04683200,0.18900000; INTO bnc_btc_eth",
            &mut state,
        );
        assert_eq!(ReturnType::String("".into()), resp);
    }

}
