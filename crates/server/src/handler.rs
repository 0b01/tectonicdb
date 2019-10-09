use crate::prelude::*;

use crate::parser;
use libtectonic::dtf::update::Update;
use std::borrow::{Cow, Borrow};

// BUG: subscribe, add, deadlock!!!

#[derive(Debug, PartialEq, Eq)]
pub enum ReturnType {
    String(Cow<'static, str>),
    Bytes(Vec<u8>),
    Error(Cow<'static, str>),
}

impl ReturnType {

    pub const HELP_STR: &'static str = "
    PING, INFO, USE [db], CREATE [db],
    ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
    FLUSH, FLUSH ALL, GET ALL, GET [count], CLEAR";

    pub fn ok() -> ReturnType {
        ReturnType::String("1".into())
    }

    pub fn string<S>(string: S) -> ReturnType
        where S: Into<Cow<'static, str>>
    {
        ReturnType::String(string.into())
    }

    pub fn error<S>(string: S) -> ReturnType
        where S: Into<Cow<'static, str>>
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
pub enum ReadLocation {
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
    Get(ReqCount, GetFormat, Range, ReadLocation),
    Count(ReqCount, ReadLocation),
    Clear(ReqCount),
    Flush(ReqCount),
    AutoFlush(bool),
    Insert(Option<Update>, Option<String>),
    Create(String),
    Subscribe(String),
    Use(String),
    Exists(String),
    Unknown,
}

#[derive(Debug)]
pub enum Event {
    NewPeer {
        sock: SocketAddr,
        stream: Arc<TcpStream>,
        shutdown: Receiver<Void>,
    },
    Command {
        from: SocketAddr,
        command: Command
    },
    History {

    }
}

/// sometimes returns string, sometimes bytes, error string
// pub type Response = (Option<String>, Option<Vec<u8>>, Option<String>);

pub fn parse_to_command(line: &str) -> Command {
    use self::Command::*;

    match line.borrow() {
        "" => Noop,
        "PING" => Ping,
        "HELP" => Help,
        "INFO" => Info,
        "PERF" => Perf,
        "COUNT" => Count(ReqCount::Count(1), ReadLocation::Fs),
        "COUNT IN MEM" => Count(ReqCount::Count(1), ReadLocation::Mem),
        "COUNT ALL" => Count(ReqCount::All, ReadLocation::Fs),
        "COUNT ALL IN MEM" => Count(ReqCount::All, ReadLocation::Mem),
        "CLEAR" => Clear(ReqCount::Count(1)),
        "CLEAR ALL" => Clear(ReqCount::All),
        "GET ALL AS JSON" => Get(ReqCount::All, GetFormat::Json, None, ReadLocation::Mem),
        "GET ALL AS CSV" => Get(ReqCount::All, GetFormat::Csv, None, ReadLocation::Mem),
        "GET ALL" => Get(ReqCount::All, GetFormat::Dtf, None, ReadLocation::Mem),
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
                let loc = if line.contains(" IN MEM") { ReadLocation::Mem } else { ReadLocation::Fs };

                Get(count, format, range, loc)
            } else {
                Unknown
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::Settings;
    use std::net;

    fn gen_state() -> (TectonicServer, SocketAddr) {
        let settings: Settings = Default::default();
        let mut global = TectonicServer::new(settings);
        let sock = SocketAddr::new(
            net::IpAddr::V4(net::Ipv4Addr::new(127, 0, 0, 1)),
            1);
        let (client_sender, _client_receiver) = mpsc::unbounded();
        global.new_connection(client_sender, sock);
        (global, sock)
    }

    #[test]
    fn should_return_pong() {
        let (mut state, sock) = gen_state();
        let resp = task::block_on(state.process_command(&Command::Ping, &sock));
        assert_eq!(ReturnType::String("PONG".into()), resp);
    }

    #[test]
    fn should_not_insert_into_empty() {
        let (mut state, sock) = gen_state();
        let resp = task::block_on(state.process_command(
            &parse_to_command("ADD 1513749530.585,0,t,t,0.04683200,0.18900000; INTO bnc_btc_eth"),
            &sock
        ));
        assert_eq!(
            ReturnType::Error("DB bnc_btc_eth not found.".into()),
            resp
        );
    }

    #[test]
    fn should_insert_ok() {
        let (mut state, sock) = gen_state();
        let resp = task::block_on(state.process_command(&parse_to_command("CREATE bnc_btc_eth"), &sock));
        assert_eq!(ReturnType::String("Created DB `bnc_btc_eth`.".into()), resp);
        let resp = task::block_on(state.process_command(
            &parse_to_command( "ADD 1513749530.585,0,t,t,0.04683200,0.18900000; INTO bnc_btc_eth"),
            &sock
        ));
        assert_eq!(ReturnType::String("".into()), resp);
    }

}
