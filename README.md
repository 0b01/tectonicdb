# tectonicdb

[![](https://img.shields.io/crates/v/tectonicdb.svg)](https://crates.io/crates/tectonicdb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/rickyhan/tectonic/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/rickyhan/tectonicdb.svg?branch=master)](https://travis-ci.org/rickyhan/tectonicdb)

tectonicdb is a very fast, highly compressed standalone datastore and streaming protocol for order book ticks.

## Reason 

This software is motivated by reducing expenditure. 1TB stored on Google Cloud PostgreSQL was too expensive and too slow. Since financial data is usually read and stored in bulk, it is possible to convert into a more efficient format.

* Uses a simple binary file format: Dense Tick Format(DTF) ...

* Stores order book tick data tuple of shape: `(timestamp, seq, is_trade, is_bid, price, size)`.

* Sorted by seq/timestamp

* 12 bytes per row

* 11.5MB per 1 million row

## Installation

There are several ways to install tectonicdb.

1. **Binaries**

Binaries are available for [download](https://github.com/rickyhan/tectonic/releases). Make sure to put the path to the binary into your PATH. Currently only released for Linux x86_64.

2. **Crates.io**

Requires Rust. Once you have Rust installed, simply run:

    cargo install tectonicdb

This will download and compile `tectonic-server` and `tectonic-cli`.

3. **GitHub**

To contribute you will need the copy of the source code on your local machine.

    git clone https://github.com/rickyhan/tectonic
    cd tectonic
    cargo build --lib
    cargo build --bin tectonic-server
    cargo build --bin tectonic-cli

The binaries can be found under `target/release/debug` folder.

## How to use

It's very easy to setup.

```
chmod +x tectonic-server
./tectonic-server --help
```

There are several commandline options:

* -a: Sets autoflush (default is false)
* -f, --dtf_folder [FOLDER]: Sets the folder to serve dtf files
* -i, --flush_interval [FLUSH_INTERVAL]: Sets autoflush interval (default every 1000 inserts)
* -g, --hist_granularity <HIST_GRANULARITY>: Sets the history record granularity interval. (default 60s)
* -h, --host <HOST>: Sets the host to connect to (default 0.0.0.0)
* -p, --port <PORT>: Sets the port to connect to (default 9001)
* -l, --log_file <LOG_FILE>: Sets the log file to write to


For example:

```
./tectonic-server -vvvvvvv -t 1000
```

This sets log verbosity to max and maximum connection to 1000.

## Monitoring

It's easy to monitor performance. The history granularity option configures the interval (in second) to periodically record item count for each data store. Then a client can call `PERF` command and retreive historical item counts.

## Logging

Log file defaults to `tectonic.log`.

## Using dtf files

Tectonic comes with a commandline tool `dtfcat` to inspect the file metadata and all the stored rows into either JSON or CSV.

Options:

```
USAGE:
    dtfcat [FLAGS] --input <INPUT>

FLAGS:
    -c, --csv         output csv
    -h, --help        Prints help information
    -m, --metadata    read only the metadata
    -V, --version     Prints version information

OPTIONS:
    -i, --input <INPUT>    file to read
```

## As a library

It is possible to use the Dense Tick Format streaming protocol / file format as a separate package. Works nicely with any buffer implementing the `Write` trait.

## Requirements

TectonicDB is a standalone service.

* Linux

* macOS

Language bindings:

- [x] TypeScript (reference implementation)

- [x] Rust

- [x] Python

- [x] JavaScript


## Additional Features

- [x] Usage statistics like Cloud SQL

- [x] Commandline inspection tool for dtf file format

- [x] Logging

- [x] Query by timestamp


# Note

This software is release under GNU General Public License which means you are **required** to contribute back and disclose source.
