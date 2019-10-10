use std::net::TcpStream;
use std::io::{Read, Write};
use std::str;

use byteorder::{BigEndian, ReadBytesExt};
use crate::dtf::file_format::{read_one_batch, decode_buffer};
use crate::dtf::update::UpdateVecConvert;
use crate::client::insert_command::InsertCommand;
use super::error::TectonicError;

/// Simple toy implementation(not meant to be used)
pub struct CxnStream {
    stream: TcpStream,
}

impl CxnStream {
    /// Consume a TcpStream
    pub fn new(stream: TcpStream) -> Self {
        CxnStream { stream }
    }

    /// Send command
    pub fn cmd(&mut self, command: &str) -> Result<String, TectonicError> {
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
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0_u8; size as usize];
            let _ = self.stream.read_exact(&mut buf);

            let mut buf = buf.as_slice();
            let v = decode_buffer(&mut buf);
            Ok(format!("[{}]\n", v.as_json()))

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
}


/// Connection to TectonicDB server
pub struct Cxn {
    stream: TcpStream,
}

impl Cxn {
    /// Create a new connection
    pub fn new(host: &str, port: &str) -> Result<Cxn, TectonicError> {
        let addr = format!("{}:{}", host, port);

        info!("Connecting to {}", addr);

        let stream = match TcpStream::connect(&addr) {
            Ok(stm) => stm,
            Err(_) => return Err(TectonicError::ConnectionError)
        };

        Ok(Cxn {
            stream,
        })
    }

    /// Create datastore
    pub fn create_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        info!("Creating db {}", dbname);
        self.cmd(&format!("CREATE {}\n", dbname))
    }

    /// Switch to datastore
    pub fn use_db(&mut self, dbname: &str) -> Result<String, TectonicError> {
        self.cmd(&format!("USE {}\n", dbname))
    }

    /// Send custom command
    pub fn cmd(&mut self, command : &str) -> Result<String, TectonicError> {
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
            let vecs = read_one_batch(&mut self.stream).unwrap();
            Ok(format!("[{}]\n", vecs.as_json()))
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

    /// Insert update or updates using InsertCommand
    pub fn insert(&mut self, cmd: InsertCommand) -> Result<(), TectonicError> {
        for cmd in &cmd.into_string() {
            let _res = self.cmd(cmd)?;
        }
        Ok(())
    }
}
