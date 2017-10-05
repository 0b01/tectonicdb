extern crate clap;
extern crate byteorder;
// mod db;
mod conf;

use clap::{Arg, App};
use std::net::TcpStream;
use std::str;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::io::{self, Read, Write};

struct Cxn {
    stream : TcpStream,
    addr: String
}

impl Cxn {
    fn cmd(&mut self, command : &str) -> String {
        let _ = self.stream.write(command.as_bytes());
        let size = self.stream.read_u64::<BigEndian>().unwrap();
        let mut buf = vec![0; size as usize];
        let _ = self.stream.read_exact(&mut buf);
        str::from_utf8(&buf).unwrap().to_owned()
    }
}

fn main() {
        let matches = App::new("tectonic-cli")
                          .version("0.0.1")
                          .author("Ricky Han <tectonic@rickyhan.com>")
                          .about("command line client for tectonic financial datastore")
                          .arg(Arg::with_name("host")
                               .short("h")
                               .long("host")
                               .value_name("HOST")
                               .help("Sets the host to connect to (default 0.0.0.0)")
                               .takes_value(true))
                          .arg(Arg::with_name("port")
                               .short("p")
                               .long("port")
                               .value_name("PORT")
                               .help("Sets the port to connect to (default 9001)")
                               .takes_value(true))
                          .arg(Arg::with_name("v")
                               .short("v")
                               .multiple(true)
                               .help("Sets the level of verbosity"))
                          .get_matches();
    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("9001");

    let verbosity = matches.occurrences_of("v");

    let mut cxn = connect(host, port, verbosity);

    loop {
        let mut cmd = String::new();
        io::stdin().read_line(&mut cmd).unwrap();
        let res = cxn.cmd(&cmd);
        print!("{}", res);
    }
}


fn connect(host : &str, port : &str, verbosity : u64) -> Cxn {
    let addr = format!("{}:{}", host, port);
    if verbosity > 0 {
        println!("Connecting to {}", addr);
    }

    let mut db = Cxn{
        stream : TcpStream::connect(&addr).unwrap(),
        addr
    };

    db
}
