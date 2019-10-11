use std::net::TcpStream;
use std::io::{Read, Write, BufRead};
use byteorder::{BigEndian, ReadBytesExt};

use bufstream::BufStream;

use libtectonic::dtf::update::Update;
use crate::error::TectonicError;
use libtectonic::dtf::{update::UpdateVecConvert, file_format::decode_buffer};

pub struct TectonicClient {
    pub stream: BufStream<TcpStream>,
}

impl TectonicClient {
    pub fn new(host: &str, port: &str) -> Result<TectonicClient, TectonicError> {
        let addr = format!("{}:{}", host, port);

        info!("Connecting to {}", addr);

        let stream = match TcpStream::connect(&addr) {
            Ok(stm) => stm,
            Err(_) => return Err(TectonicError::ConnectionError)
        };

        let stream = BufStream::new(stream);

        Ok(TectonicClient {
            stream,
        })
    }

    unsafe fn cmd_bytes_no_check(&mut self, command: &[u8]) -> Result<bool, TectonicError> {
        self.stream.write(command)?;
        self.stream.flush()?;
        self.stream.read_u8()
            .map(|i| i == 0x1)
            .map_err(|_| TectonicError::ConnectionError)
    }

    fn stream_send_cmd(&mut self, command: &str) -> Result<String, TectonicError> {
        self.stream.write(command.as_bytes())?;
        self.stream.flush()?;

        let success = match self.stream.read_u8() {
            Ok(re) => re == 0x1,
            Err(_) => return Err(TectonicError::ConnectionError),
        };

        if command.starts_with("GET")
            && !command.contains("AS CSV")
            && !command.contains("AS JSON")
            && success
        {
            let size = self.stream.read_u64::<BigEndian>()?;
            let mut buf = vec![0_u8; size as usize];
            self.stream.read_exact(&mut buf)?;

            let mut buf = buf.as_slice();
            let v = decode_buffer(&mut buf);
            Ok(format!("[{}]\n", v.as_json()))
        } else {
            let size = self.stream.read_u64::<BigEndian>()?;
            let mut buf = vec![0; size as usize];
            self.stream.read_exact(&mut buf)?;
            let res = std::str::from_utf8(&buf).unwrap().to_owned();
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

    pub fn create_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        info!("Creating db {}", dbname);
        self.cmd(&format!("CREATE {}\n", dbname))
    }

    pub fn use_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        self.cmd(&format!("USE {}\n", dbname))
    }

    pub fn cmd(&mut self, command: &str) -> Result<String, TectonicError> {
        self.stream_send_cmd(command)
    }

    pub fn subscribe(&mut self, dbname: &str) -> Result<(), TectonicError> {
        let ret = self.cmd(&format!("SUBSCRIBE {}\n", dbname))?;
        loop {
            let success = match self.stream.read_u8() {
                Ok(re) => re == 0x1,
                Err(_) => return Err(TectonicError::ConnectionError),
            };

            let size = self.stream.read_u64::<BigEndian>()?;
            let mut buf = vec![0; size as usize];
            self.stream.read_exact(&mut buf)?;
            let res = std::str::from_utf8(&buf).unwrap().to_owned();
            println!("{}", res);
        }
    }

    #[deprecated]
    pub fn insert_text(&mut self, book_name: String, update: &Update) -> Result<String, TectonicError> {
        let is_trade = if update.is_trade {"t"} else {"f"};
        let is_bid = if update.is_bid {"t"} else {"f"};
        let cmdstr = format!("ADD {}, {}, {}, {}, {}, {}; INTO {}\n",
                        update.ts, update.seq, is_trade, is_bid, update.price, update.size, book_name);
        self.cmd(&cmdstr)
    }

    pub fn insert(&mut self, book_name: Option<String>, update: &Update) -> Result<bool, TectonicError> {
        let buf = libtectonic::utils::encode_insert_into(&book_name, update)?;
        unsafe {
            self.cmd_bytes_no_check(&buf)
        }
    }

    pub fn shutdown(self) {
        self.stream.into_inner().unwrap().shutdown(std::net::Shutdown::Both).unwrap()
    }
}
