# Getting started {#getting-started}

To install TectonicDB, simply download the [latest build](https://github.com/rickyhan/tectonicdb/releases).

#### Ubuntu 16.04

```bash
mkdir tectonic && cd tectonic
wget https://github.com/rickyhan/tectonicdb/releases/download/0.2/tectonic-server
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

There are currently several API clients:

1. Python
2. Javascript
3. Rust



