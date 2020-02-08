# tectonicdb

[![Build Status](https://travis-ci.org/0b01/tectonicdb.svg?branch=master)](https://travis-ci.org/0b01/tectonicdb)
[![crate.io](https://img.shields.io/crates/v/tdb.svg)](https://crates.io/crates/tdb)
[![doc.rs](https://docs.rs/tdb-core/badge.svg)](https://docs.rs/crate/tdb-core)
![Minimum Rust version](https://img.shields.io/badge/rustc-1.40+-yellow.svg)
![Rust stable](https://img.shields.io/badge/rust-stable-green.svg)


tectonicdb is a fast, highly compressed standalone database and streaming protocol for order book ticks.

## Why

* Uses a simple and efficient binary file format: Dense Tick Format(DTF)

* Stores order book tick data tuple of shape: `(timestamp, seq, is_trade, is_bid, price, size)`.

* Sorted by timestamp + seq

* 12 bytes per orderbook event

* 600,000 inserts per thread second

## Installation

There are several ways to install tectonicdb.

1.  **Binaries**

Binaries are available for [download](https://github.com/0b01/tectonicdb/releases). Make sure to put the path to the binary into your PATH. Currently only build is for Linux x86_64.

2.  **Crates**

    cargo install tdb tdb-server

This command will download `tdb` and `tdb-server` from crates.io and build locally.

3.  **GitHub**

To contribute you will need the copy of the source code on your local machine.

    git clone https://github.com/0b01/tectonicdb
    cd tectonicdb
    cargo build --release
    cargo run --release tdb-server

The binaries can be found under `target/` folder.

## How to use

It's very easy to setup.

```
chmod +x tdb-server
./tdb-server --help
```

For example:

```bash
./tdb-server -vv -a -i 10000
# run the server on INFO verbosity
# turn on autoflush for every 10000 inserts per orderbook
```

### Configuration

To config the Google Cloud Storage and Data Collection Backend integration, the following environment variables are used:

| Variable Name                 | Default      | Description                                                                                                                                   |
| ----------------------------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `TDB_HOST`             | 0.0.0.0      | The host to which the database will bind                                                                                                      |
| `TDB_PORT`             | 9001         | The port that the database will listen on                                                                                                     |
| `TDB_DTF_FOLDER`       | db           | Name of the directory in which DTF files will be stored                                                                                       |
| `TDB_AUTOFLUSH`        | false        | If `true`, recorded orderbook data will automatically be flushed to DTF files every `interval` inserts.                                       |
| `TDB_FLUSH_INTERVAL`   | 1000         | Every `interval` inserts, if `autoflush` is enabled, DTF files will be written from memory to disk.                                           |
| `TDB_GRANULARITY`      | 0            | Record history granularity level                                                                                                              |
| `TDB_LOG_FILE_NAME`    | tdb.log      | Filename of the log file for the database                                                                                                     |
| `TDB_Q_CAPACITY`       | 300          | Capacity of the circular queue for recording history                                                                                          |

## Client API

| Command | Description |
| :--- | :--- |
| HELP | Prints help |
| PING | Responds PONG |
| INFO | Returns info about table schemas |
| PERF | Returns the answercount of items over time |
| LOAD \[orderbook\] | Load orderbook from disk to memory |
| USE \[orderbook\] | Switch the current orderbook |
| CREATE \[orderbook\] | Create orderbook |
| GET \[n\] FROM \[orderbook\] | Returns items |
| GET \[n\] | Returns n items from current orderbook |
| COUNT | Count of items in current orderbook |
| COUNT ALL | Returns total count from all orderbooks |
| CLEAR | Deletes everything in current orderbook |
| CLEAR ALL | Drops everything in memory |
| FLUSH | Flush current orderbook to "Howdisk can|
| FLUSHALL | Flush everything from memory to disk |
| SUBSCRIBE \[orderbook\] | Subscribe to updates from orderbook |
| EXISTS \[orderbook\] | Checks if orderbook exists |
| SUBSCRIBE \[orderbook\] | Subscribe to orderbook |

### Data commands

```
USE [dbname]
ADD [ts], [seq], [is_trade], [is_bid], [price], [size];
INSERT 1505177459.685, 139010, t, f, 0.0703620, 7.65064240; INTO dbname
```

## Monitoring

TectonicDB supports monitoring/alerting by periodically sending its usage info to an InfluxDB instance:

```bash
    --influx-db <influx_db>                        influxdb db
    --influx-host <influx_host>                    influxdb host
    --influx-log-interval <influx_log_interval>    influxdb log interval in seconds (default is 60)
```

As a concrete example,

```bash
...
$ influx
> CREATE DATABASE market_data;
> ^D
$ tdb --influx-db market_data --influx-host http://localhost:8086 --influx-log-interval 20
...
```

TectonicDB will send values `disk={COUNT_DISK},size={COUNT_MEM}` with tag `ob={ORDERBOOK}` to `market_data` measurement which is the same as the dbname.

Additionally, you can query usage information directly with `INFO` and `PERF` commands:

1. `INFO` reports the current tick count in memory and on disk.

2. `PERF` returns recorded tick count history whose granularity can be configured.

## Logging

Log file defaults to `tdb.log`.

## Testing

```bash
export RUST_TEST_THREADS=1
cargo test
```

Tests must be run sequentially because some tests depend on dtf files that other tests generate.

## Benchmark

tdb client comes with a benchmark mode. This command inserts 1M records into the tdb.

```bash
tdb -b 1000000
```
## Using dtf files

Tectonic comes with a commandline tool `dtfcat` to inspect the file metadata and all the stored events into either JSON or CSV.

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

* [x] TypeScript

* [x] Rust

* [x] Python

* [x] JavaScript

## Additional Features

* [x] Usage statistics like Cloud SQL

* [x] Commandline inspection tool for dtf file format

* [x] Logging

* [x] Query by timestamp

# Changelog

* 0.5.0: InfluxDB monitoring plugin and improved command line arguments
* 0.4.0: iterator-based APIs for handling DTF files and various quality of life improvements
* 0.3.0: Refactor to async
