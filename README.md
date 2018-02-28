# tectonicdb

[![Build Status](https://travis-ci.org/rickyhan/tectonicdb.svg?branch=master)](https://travis-ci.org/rickyhan/tectonicdb)
[![Crates.io](https://img.shields.io/crates/v/tectonicdb.svg)](https://crates.io/crates/tectonicdb)

tectonicdb is a fast, highly compressed standalone datastore and streaming protocol for order book ticks.

## Why 

This software is motivated by reducing expenditure. 1TB stored on Google Cloud PostgreSQL was too expensive and too slow. Since financial data is usually read and stored in bulk, it is possible to convert into a more efficient format.

* Uses a simple binary file format: Dense Tick Format(DTF)

* Stores order book tick data tuple of shape: `(timestamp, seq, is_trade, is_bid, price, size)`.

* Sorted by timestamp + seq

* 12 bytes per row

## Installation

There are several ways to install tectonicdb.

1. **Binaries**

Binaries are available for [download](https://github.com/rickyhan/tectonic/releases). Make sure to put the path to the binary into your PATH. Currently only build is for Linux x86_64.

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

For example:

```bash
./tectonic-server -vv -a -i 10000
# run the server on INFO verbosity
# turn on autoflush and flush every 10000
```

This sets log verbosity to max and maximum connection to 1000.

## Monitoring

There is a history granularity option that sets the interval (in second) to periodically record item count for each data store. Then a client can call `PERF` command and retreive historical item counts in JSON.

## Logging

Log file defaults to `tectonic.log`.

## Testing

```bash
export RUST_TEST_THREADS=1
```
Tests must be run sequentially because of file dependencies issues: some tests generate dtf file for others.

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

It is possible to use the Dense Tick Format streaming protocol / file format in a different application. Works nicely with any buffer implementing the `Write` trait.

## Requirements

TectonicDB is a standalone service.

* Linux

* macOS

Language bindings:

- [x] TypeScript

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
