extern crate byteorder;
extern crate dtf;

use std::net::TcpStream;
use std::str;
use self::byteorder::{BigEndian, /*WriteBytesExt, */ ReadBytesExt};
use std::io::{Read, Write};
use std::error;
use std::fmt;
use std::{thread, time};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Mutex, Arc, RwLock};

#[derive(Debug)]
pub enum TectonicError {
    ServerError(String),
    ConnectionError,
}

impl error::Error for TectonicError {
    fn description(&self) -> &str {
        match self {
            &TectonicError::ServerError(ref msg) => &msg,
            &TectonicError::ConnectionError => "disconnection from tectonicdb",
        }
    }
}

impl fmt::Display for TectonicError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &TectonicError::ServerError(ref msg) => write!(f, "TectonicError: {}", msg),
            &TectonicError::ConnectionError => write!(f, "ConnectionError"),
        }
    }
}

struct CxnStream{stream: TcpStream}

impl CxnStream {
    fn cmd(&mut self, command: &str) -> Result<String, TectonicError> {

        let _ = self.stream.write(command.as_bytes());
        // TODO: deadlock!
        let success = match self.stream.read_u8() {
            Ok(re) => re == 0x1,
            Err(_) => return Err(TectonicError::ConnectionError),
        };

        if command.starts_with("GET") && !command.contains("AS JSON") && success {
            let vecs = dtf::read_one_batch(&mut self.stream);
            Ok(format!("[{}]\n", dtf::update_vec_to_json(&vecs)))
        } else {
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0; size as usize];
            let _ = self.stream.read_exact(&mut buf);
            let res = str::from_utf8(&buf).unwrap().to_owned();
            if success {
                Ok(res)
            } else {
                Err(TectonicError::ServerError(res))
            }
        }
    }
}

pub struct Cxn {
    stream: Arc<RwLock<CxnStream>>,
    pub subscription: Option<Arc<Mutex<Receiver<String>>>>,
}

impl Cxn {
    pub fn new(host: &str, port : &str, verbosity : u8) -> Result<Cxn, TectonicError> {
        let addr = format!("{}:{}", host, port);

        let stream = match TcpStream::connect(&addr) {
            Ok(stm) => stm,
            Err(_) => return Err(TectonicError::ConnectionError)
        };

        Ok(Cxn {
            stream: Arc::new(RwLock::new(CxnStream{stream: stream})),
            subscription: None,
        })
    }

    pub fn subscribe(&mut self, dbname: &str) -> Result<(), TectonicError> {
        let _ = self.cmd(&format!("SUBSCRIBE {}", dbname))?;

        let streamcopy = self.stream.clone();
        let (tx, rx) = channel();

        let tx = Arc::new(Mutex::new(tx));
        let rx = Arc::new(Mutex::new(rx));

        thread::spawn(move || {
            loop {
                let res = streamcopy.write().unwrap().cmd("\n").unwrap();
                println!("{}", res);
                if res == "NONE\n" {
                    thread::sleep(time::Duration::from_millis(1));
                } else {
                    let _ = tx.lock().unwrap().send(res);
                }
            }
        });

        self.subscription = Some(rx);

        Ok(())
    }

    pub fn cmd(&mut self, command : &str) -> Result<String, TectonicError> {
        self.stream.write().unwrap().cmd(command)
    }

    pub fn insert(&mut self, cmd: InsertCommand) -> Result<(), TectonicError> {
        for cmd in &cmd.into_string() {
            let _res = self.cmd(cmd)?;
        }
        Ok(())
    }
}

pub struct CxnPool{
    cxns: Vec<Cxn>,
    host: String,
    port: String,
    verbosity: u8,
    available_workers: VecDeque<usize>,
    insert_retry_queue: Vec<InsertCommand>,
}

use std::collections::VecDeque;
impl CxnPool {
    pub fn new(n: usize, host: &str, port : &str, verbosity : u8) -> Result<Self, TectonicError> {
        let mut v = vec![];
        let mut q = VecDeque::new();

        for i in 0..n {
            let cxn = Cxn::new(host, port, verbosity)?;
            v.push(cxn);
            q.push_back(i);
        }

        Ok(CxnPool{
            cxns: v,
            host: host.to_owned(),
            port: host.to_owned(),
            verbosity,
            available_workers: q,
            insert_retry_queue: vec![],
        })
    }

    pub fn cmd(&mut self, command: &str) -> Result<String, TectonicError> {
        let n = self.available_workers.pop_front();
        let n = match n {
            Some(n) => n,
            None => {
                // grow avail cxns
                self.cxns.push(Cxn::new(&self.host, &self.port, self.verbosity)?);
                self.cxns.len()
            }
        };

        // exec command
        let result = self.cxns[n].cmd(command);
        let ret = match result {
            Err(TectonicError::ConnectionError) => {
                thread::sleep(time::Duration::from_secs(1));
                // replace current cxn
                self.cxns[n] = Cxn::new(&self.host, &self.port, self.verbosity)?;
                result
            }
            _ => result,
        };

        // push id back to queue of avail workers
        self.available_workers.push_back(n);

        ret
    }

    pub fn insert(&mut self, cmd: &InsertCommand) -> Result<(), TectonicError> {

        for i in self.insert_retry_queue.pop() { let _ = self.insert(&i)?; }

        let n = self.available_workers.pop_front();
        let n = match n {
            Some(n) => n,
            None => {
                // grow avail cxns
                self.cxns.push(Cxn::new(&self.host, &self.port, self.verbosity)?);
                self.cxns.len()
            }
        };

        // self.insert_retry_queue.push(cmd.clone());

        for c in cmd.clone().into_string() {
            let result = self.cxns[n].cmd(&c);
            match result {
                Err(TectonicError::ConnectionError) => {
                    thread::sleep(time::Duration::from_secs(1));
                    self.insert_retry_queue.push(cmd.clone());
                    self.cxns[n] = Cxn::new(&self.host, &self.port, self.verbosity)?;
                    return Err(TectonicError::ConnectionError);
                },
                Err(TectonicError::ServerError(msg)) => {
                },
                _ => (),
            }
        }

        // self.insert_retry_queue.pop();

        // push id back to queue of avail workers
        self.available_workers.push_back(n);

        Ok(())
    }
}

#[derive(Clone)]
pub enum InsertCommand {
    Add(String, dtf::Update),
    BulkAdd(String, Vec<dtf::Update>),
}

impl InsertCommand {
    pub fn into_string(self) -> Vec<String> {
        match self {
            InsertCommand::Add(dbname, up) => {
                let is_trade = if up.is_trade {"t"} else {"f"};
                let is_bid = if up.is_bid {"t"} else {"f"};
                let s = format!("ADD {}, {}, {}, {}, {}, {}; INTO {}\n",
                                up.ts, up.seq, is_trade, is_bid, up.price, up.size, dbname
                );
                vec![s]
            },
            InsertCommand::BulkAdd(dbname, ups) => {
                let mut cmds = vec![format!("BULKADD INTO {}\n", dbname)];
                for up in ups {
                    cmds.push(format!("{}, {}, {}, {}, {}, {};\n",
                            up.ts, up.seq, up.is_trade, up.is_bid, up.price, up.size));
                }

                cmds.push("DDAKLUB\n".to_owned());
                cmds
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_err() {
        let mut cxn = Cxn::new("localhost", "9001", 3).unwrap();
        let res = cxn.cmd("USE test\n");
        assert!(res.is_err());
    }

    #[test]
    fn should_cxnpool_work() {
        let mut cxn = CxnPool::new(10, "localhost", "9001", 3).unwrap();
        let res = cxn.cmd("COUNT ALL\n").unwrap();
        // assert_eq!("3\n", res);

        let res = cxn.insert(&InsertCommand::Add("default".to_owned(), dtf::Update {
            ts: 0,
            seq: 0,
            is_bid: false,
            is_trade: false,
            price: 0.,
            size: 0.,
        }));
        println!("{:?}", res);
    }
}
