# Getting started {#getting-started}

To install TectonicDB, simply download the [latest build](https://github.com/0b01/tectonicdb/releases).

#### Ubuntu 16.04

```bash
mkdir tectonic && cd tectonic
wget https://github.com/0b01/tectonicdb/releases/download/0.2/tectonic-server
chmod +x tectonic-server
./tectonic-server --help
./tectonic-server -p 9002 -f db -vv
```

TectonicDB communicates over TCP:

```bash
$ nc localhost 9002
INFO
```

Now that you have installed TectonicDB, you can now hook up a market data pipeline. To do so, you will need to use a client api.

Currently these language bindings are available:

1. [Python 3.6 (async generator)](https://github.com/0b01/tectonicdb/blob/master/cli/python/tectonic.py)
2. [Javascript](https://github.com/0b01/tectonicdb/blob/master/cli/tectonicjs/src/tectonic.ts)
3. [Rust](https://github.com/0b01/tectonicdb/blob/master/cli/db.rs)
