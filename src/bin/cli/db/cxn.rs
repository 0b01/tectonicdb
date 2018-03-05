use db::TectonicError;
use std::net::TcpStream;
use std::io::{Read, Write};
use byteorder::{BigEndian, /*WriteBytesExt, */ ReadBytesExt};
use db::insert_command::InsertCommand;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, RwLock, Mutex};
use std::thread;
use std::time;


use dtf;
use std::str;


struct CxnStream {
    stream: TcpStream,
}

impl CxnStream {
    fn cmd(&mut self, command: &str) -> Result<String, TectonicError> {
        let _ = self.stream.write(command.as_bytes());

        let success = match self.stream.read_u8() {
            Ok(re) => re == 0x1,
            Err(_) => return Err(TectonicError::ConnectionError),
        };

        if command.starts_with("GET")
            && !command.contains("AS CSV")
            && !command.contains("AS JSON")
            && success
        {
            let vecs = dtf::read_one_batch(&mut self.stream).unwrap();
            Ok(format!("[{}]\n", dtf::update_vec_to_json(&vecs)))
        } else {
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0; size as usize];
            let _ = self.stream.read_exact(&mut buf);
            let res = str::from_utf8(&buf).unwrap().to_owned();
            if success {
                Ok(res)
            } else if res.contains("ERR: DB") {
                let dbname = res.split(" ").nth(2).unwrap();
                Err(TectonicError::DBNotFoundError(dbname.to_owned()))
            } else  {
                Err(TectonicError::ServerError(res))
            }
        }
    }

    fn new(stream : TcpStream) -> Self {
        CxnStream { stream }
    }
}


pub struct Cxn {
    stream: Arc<RwLock<CxnStream>>,
    pub subscription: Option<Arc<Mutex<Receiver<String>>>>,
}

impl Cxn {
    pub fn new(host: &str, port: &str) -> Result<Cxn, TectonicError> {
        let addr = format!("{}:{}", host, port);

        info!("Connecting to {}", addr);

        let stream = match TcpStream::connect(&addr) {
            Ok(stm) => stm,
            Err(_) => return Err(TectonicError::ConnectionError)
        };

        Ok(Cxn {
            stream: Arc::new(RwLock::new(CxnStream::new(stream))),
            subscription: None,
        })
    }

    pub fn create_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        info!("Creating db {}", dbname);
        self.cmd(&format!("CREATE {}\n", dbname))
    }

    pub fn use_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        self.cmd(&format!("USE {}\n", dbname))
    }

    pub fn cmd(&mut self, command : &str) -> Result<String, TectonicError> {
        self.stream.write().unwrap().cmd(command)
    }

    pub fn subscribe(&mut self, dbname: &str) -> Result<(), TectonicError> {
        let _ = self.cmd(&format!("SUBSCRIBE {}", dbname))?;

        let streamcopy = self.stream.clone();
        let (tx, rx) = channel();

        let tx = Arc::new(Mutex::new(tx));
        let rx = Arc::new(Mutex::new(rx));

        thread::spawn(move || loop {
            let res = streamcopy.write().unwrap().cmd("\n").unwrap();
            println!("{}", res);
            if res == "NONE\n" {
                thread::sleep(time::Duration::from_millis(1));
            } else {
                let _ = tx.lock().unwrap().send(res);
            }
        });

        self.subscription = Some(rx);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn insert(&mut self, cmd: InsertCommand) -> Result<(), TectonicError> {
        for cmd in &cmd.into_string() {
            let _res = self.cmd(cmd)?;
        }
        Ok(())
    }
}

