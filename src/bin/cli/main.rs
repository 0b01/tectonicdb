extern crate clap;
extern crate byteorder;
extern crate dtf;

use clap::{Arg, App};
use std::net::TcpStream;
use std::str;
use byteorder::{BigEndian, /*WriteBytesExt, */ ReadBytesExt};
use std::io::{self, Read, Write};
use std::time;

struct Cxn {
    stream : TcpStream,
    // addr: String
}

impl Cxn {
    fn cmd(&mut self, command : &str) -> String {
        let _ = self.stream.write(command.as_bytes());
        let success = self.stream.read_u8().unwrap() == 0x1;
        if command.starts_with("GET") && !command.contains("AS JSON") && success {
            let vecs = dtf::read_one_batch(&mut self.stream);
            format!("[{}]\n", dtf::update_vec_to_json(&vecs))
        } else {
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0; size as usize];
            let _ = self.stream.read_exact(&mut buf);
            str::from_utf8(&buf).unwrap().to_owned()
        }
    }
    fn new(host : &str, port : &str, verbosity : u64) -> Cxn {
        let addr = format!("{}:{}", host, port);

        if verbosity > 0 {
            println!("Connecting to {}", addr);
        }

        let cxn = Cxn{
            stream : TcpStream::connect(&addr).unwrap(),
            // addr
        };

        cxn
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
                          .arg(Arg::with_name("b")
                               .short("b")
                               .value_name("ITERATION")
                               .multiple(false)
                               .help("Benchmark network latency")
                               .takes_value(true))
                          .get_matches();
    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("9001");
    let verbosity = matches.occurrences_of("v");

    let mut cxn = Cxn::new(host, port, verbosity);
    
    let mut t = time::SystemTime::now();
    if matches.is_present("b") {
        let times = matches.value_of("b")
                    .unwrap_or("10")
                    .parse::<usize>()
                    .unwrap_or(10) + 1;

        for _ in 1..times {
            let res = cxn.cmd("ADD 1513922718770, 0, t, f, 0.001939, 22.85; INTO bnc_gas_btc\n");
            println!("res: {:?}, latency: {:?}", res, t.elapsed());
            t = time::SystemTime::now();
        }
    } else {
        loop {
            print!("--> ");
            io::stdout().flush().ok().expect("Could not flush stdout"); // manually flush stdout

            let mut cmd = String::new();
            io::stdin().read_line(&mut cmd).unwrap();
            let res = cxn.cmd(&cmd);
            print!("{}", res);
        }
    }
}

