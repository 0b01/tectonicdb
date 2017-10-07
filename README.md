# tectonicdb

![](https://img.shields.io/crates/v/tectonicdb.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/rickyhan/tectonic/blob/master/LICENSE)

tectonicdb is a very fast, highly compressed standalone datastore and streaming protocol for order book ticks. Achieves ~50x compression on disk usage, ~100x compression on bandwidth usage compared to PostgreSQL. Easily scale to billions of records since file read is cheap and fast.

## Raison d'etre

This software is motivated by reducing expenditure. 1TB stored on Google Cloud PostgreSQL was too expensive and too slow. Since financial data is usually read and stored in bulk, it is possible to convert into a more efficient format.

* Uses a simple binary file format: Dense Tick Format(DTF) ...

* ... to store order book tick data tuple of shape: `(timestamp, seq, is\_trade, is\_bid, price, size)`.

* Automatically sorted by seq/timestamp

* 13 bytes per row or ...

* 12.5MB per 1 million row

## Installation

There are several ways to install tectonicdb.

1. **Binaries**

Binaries are available for [download](https://github.com/rickyhan/tectonic/releases). Make sure to put the path to the binary into your PATH. Currently only released for Linux x86_64.

2. **Crates.io**

Requires Rust. Once you have Rust installed, simply run:

    cargo install tectonicdb

This will download and compile `tectonic-server` and `tectonic-cli`.

3. **From GitHub**

To contribute you will need the copy of the source code on your local machine.

    git clone https://github.com/rickyhan/tectonic
    cd tectonic
    cargo build --lib
    cargo build --bin tectonic-server
    cargo build --bin tectonic-cli

The binaries can be found under `target/release/debug` folder.


## As a library

It is possible to use the `dtf` protocol/format as a separate package. Works nicely with any buffer implementing the `Write` trait.

## Requirements

TectonicDB is a standalone service.

Operating system:

* Ubuntu

* macOS

Language bindings:

* [*] Rust
* [ ] Python
* [ ] JavaScript


## Additional Features

* [ ] Usage statistics like Cloud SQL
* [ ] Query by timestamp
