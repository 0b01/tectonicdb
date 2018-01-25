## TectonicDB

TectonicDB is a fast, highly compressed standalone datastore and streaming protocol for order book ticks. It is the first open source database in this space.

## Rationale

This software is motivated by reducing expenditure. 1TB stored on Google Cloud PostgreSQL was too expensive and too slow. Since financial data is usually read and stored in bulk, it is possible to convert into a more efficient format.

* Uses a simple binary file format: Dense Tick Format\(DTF\)

* Stores order book tick data tuple of shape:`(timestamp, seq, is_trade, is_bid, price, size)`.

* Sorted by timestamp + seq

* 12 bytes per row

## Stability

I have been running TectonicDB and a proprietary market data connector for a few months.

```
$ ifconfig
          RX bytes:661346815971 (661.3 GB) TX bytes:367399985903 (367.3 GB)
$ uptime
 19:51:43 up 31 days, 13:41,  5 users,  load average: 0.18, 0.20, 0.13
```

## Docs

To build this documentations, `gitbook serve`.
