use state::*;
use parser;
use dtf::Update;

#[derive(Debug)]
pub enum ReturnType {
    String(String),
    Bytes(Vec<u8>),
    Error(String)
}

#[derive(Debug)]
enum ReqCount {
    All,
    Count(u32)
}

#[derive(Debug)]
enum GetFormat {
    JSON,
    DTF
}

type DbName = String;

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
    Get(ReqCount, GetFormat, Option<(u32,u32)>),
    Count(ReqCount),
    Clear(ReqCount),
    Flush(ReqCount),
    Insert(Option<Update>, Option<DbName>),
    Create(DbName),
    Use(DbName),
    Exists(DbName),
    Unknown
}

static HELP_STR : &str = "PING, INFO, USE [db], CREATE [db],
ADD [ts],[seq],[is_trade],[is_bid],[price],[size];
BULKADD ...; DDAKLUB
FLUSH, FLUSHALL, GETALL, GET [count], CLEAR
";

/// sometimes returns string, sometimes bytes, error string
// pub type Response = (Option<String>, Option<Vec<u8>>, Option<String>);

pub fn gen_response (string : &str, state: &mut State) -> ReturnType {
    use self::Command::*;

    let command: Command = match string {
        "" => Nothing,
        "PING" => Ping,
        "HELP" => Help,
        "INFO" => Info,
        "PERF" => Perf,
        "BULKADD" => BulkAdd,
        "DDAKLUB" => BulkAddEnd,
        "COUNT" => Count(ReqCount::Count(1)), 
        "COUNT ALL" => Count(ReqCount::All),
        "CLEAR" => Clear(ReqCount::Count(1)),
        "CLEAR ALL" => Clear(ReqCount::All),
        "GET ALL AS JSON" => Get(ReqCount::All, GetFormat::JSON, None),
        "GET ALL" => Get(ReqCount::All, GetFormat::DTF, None),
        "FLUSH" => Flush(ReqCount::Count(1)),
        "FLUSH ALL" => Flush(ReqCount::All),
        _ => {
            // is in bulkadd
            if state.is_adding {
                let parsed = parser::parse_line(string);
                let current_db = state.bulkadd_db.clone();
                let dbname = current_db.unwrap();
                Insert(parsed, Some(dbname))
            } else

            if string.starts_with("BULKADD INTO ") {
                let (_index, dbname) = parser::parse_dbname(string);
                BulkAddInto(dbname.to_owned())
            } else 

            if string.starts_with("CREATE ") {
                let dbname : &str = &string[7..];
                Create(dbname.to_owned())
            } else

            if string.starts_with("USE ") {
                let dbname : &str = &string[4..];
                Use(dbname.to_owned())
            } else

            if string.starts_with("EXISTS ") {
                let dbname : &str = &string[7..];
                Exists(dbname.to_owned())
            } else

            if string.starts_with("ADD ") {
                let parsed = if string.contains(" INTO ") {
                        parser::parse_add_into(&string)
                    } else {
                        let data_string : &str = &string[3..];
                        match parser::parse_line(&data_string) {
                            Some(up) => (Some(up), Some(state.current_store_name.to_owned())),
                            None => (None, None)
                        }
                    };
                Insert(parsed.0, parsed.1)
            } else


            // get
            if string.starts_with("GET ") {
                // how many records from memory we want...
                let count : &str = &string.clone()[4..];
                let count : Vec<&str> = count.split(" ").collect();
                let count = count[0].parse::<u32>().unwrap();

                let ranged = string.contains(" FROM ");
                let range = if ranged {
                        // range to query
                        let from_epoch = string.clone()[(string.find(" FROM ").unwrap()+6)..]
                                        .split(" ")
                                        .collect::<Vec<&str>>()
                                        [0]
                                        .parse::<u32>()
                                        .unwrap();
                        let to_epoch = string.clone()[(string.find(" TO ").unwrap()+4)..]
                                        .split(" ")
                                        .collect::<Vec<&str>>()
                                        [0]
                                        .parse::<u32>()
                                        .unwrap();
                        Some((from_epoch, to_epoch))
                    } else {
                        None
                    };

                // test if json
                let format =  if string.contains(" AS JSON") { GetFormat::JSON } else { GetFormat::DTF };

                Get(ReqCount::Count(count), format, range)
            } else

            { Unknown }
        }
    };

    match command {
        Nothing =>
            return_string(""),
        Ping =>
            return_string("PONG"),
        Help =>
            return_string(HELP_STR),
        Info =>
            return_string(&state.info()),
        Perf =>
            return_string(&state.perf()),
        BulkAdd => 
            {
                state.is_adding = true;
                return_string("")
            },
        BulkAddInto(dbname) =>
            {
                state.bulkadd_db = Some(dbname);
                state.is_adding = true;
                return_string("")
            },
        BulkAddEnd => 
            {
                state.is_adding = false;
                state.bulkadd_db = None;
                return_string("1")
            },
        Count(ReqCount::Count(_)) => 
            return_string(&format!("{}", state.count())),
        Count(ReqCount::All) => 
            return_string(&format!("{}", state.countall())),
        Clear(ReqCount::Count(_)) => 
            {
                state.clear();
                return_string("1")
            },
        Clear(ReqCount::All) => 
            {
                state.clearall();
                return_string("1")
            },
        Flush(ReqCount::Count(_)) =>
            {
                state.flush();
                return_string("1")
            },
        Flush(ReqCount::All) =>
            {
                state.flushall();
                return_string("1")
            },

        // update, dbname
        Insert(Some(up), Some(dbname)) =>
            {
                state.insert(up, &dbname);
                return_string("")
            },
        Insert(Some(up), None) =>
            {
                state.add(up);
                return_string("")
            },
        Insert(None, _) => 
            return_err("Unable to parse line"),


        Create(dbname) =>
            { 
                state.create(&dbname); 
                return_string(&format!("Created DB `{}`.", &dbname))
            },
        Use(dbname) => 
            {
                match state.use_db(&dbname) {
                    Some(_) => return_string(&format!("SWITCHED TO DB `{}`.", &dbname)),
                    None => return_err(&format!("No db named `{}`", dbname))
                }
            },
        Exists(dbname) =>
            {
                if state.exists(&dbname) {
                    return_string("1")
                } else {
                    return_err(&format!("No db named `{}`", dbname))
                }
            },

        // get
        Get(ReqCount::All, GetFormat::JSON, _) => 
            {
                match state.get_n_as_json(None) {
                    Some(json) => return_string(&json),
                    None => return_err("Not enough items to return."),
                }
            },
        Get(ReqCount::All, GetFormat::DTF, _) => 
            {
                match state.get(None) {
                    Some(bytes) => return_bytes(bytes),
                    None => return_err("Failed to GET ALL.")
                }
            },
        Get(ReqCount::Count(count), GetFormat::JSON, range) => 
            {
                match range {
                    Some((min, max)) => unimplemented!(),
                    None => {
                        match state.get_n_as_json(Some(count)) {
                            Some(json) => return_string(&json),
                            None => return_err(&format!("Requested {} items. Too many.", count))
                        }
                    }
                }
            }

        Get(ReqCount::Count(count), GetFormat::DTF, range) => 
            {
                match range {
                    Some((min, max)) => unimplemented!(),
                    None => {
                        match state.get(Some(count)) {
                            Some(bytes) => return_bytes(bytes),
                            None => return_string(&format!("Failed to get {}.", count))
                        }
                    }
                }
            }

        Unknown => 
            return_err("Unknown command.")
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