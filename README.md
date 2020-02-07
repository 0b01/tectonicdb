# tectonicdb

[![Build Status](https://travis-ci.org/0b01/tectonicdb.svg?branch=master)](https://travis-ci.org/0b01/tectonicdb)

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
| `GCLOUD_OAUTH_TOKEN`          | _unset_      | Token used to authenticate with Google Cloud for uploading DTF files                                                                          |
| `GCLOUD_BUCKET_NAME`          | `tick_data`  | Name of the bucket in which uploaded DTF files are stored                                                                                     |
| `GCLOUD_FOLDER`               | _unset_      | Name of the folder inside of the bucket into which the DTF files are stored                                                                   |
| `GCLOUD_REMOVE_ON_UPLOAD`     | true         | If true, the uploaded DTF files are deleted after upload                                                                                      |
| `GCLOUD_UPLOAD_INTERVAL_SECS` | 30           | Every `n` seconds, all files over `GCLOUD_MIN_FILE_SIZE_BYTES` will be uploaded to Google Cloud Storage and their metadata posted to the DCB. |
| `GCLOUD_MIN_FILE_SIZE_BYTES`  | 1024 \* 1024 | Files over this size in bytes will be uploaded every 30 seconds                                                                               |
| `DCB_URL`                     | _unset_      | The URL of the Data Collection Backend's batch ingestion endpoint (leave unset if you don't know what the DCB is or aren't using it)          |
| `DTF_METADATA_TAGS`           | `""`         | An array of tags that will be included in metadata for DTF files                                                                              |
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

There is a history granularity option that sets the interval (in second) to periodically record item count for each orderbook, which then can be retrieved by issuing a `PERF` command.

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

* 0.3.0: Refactor to async
