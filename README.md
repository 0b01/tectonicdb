# tectonic

This is a very fast, highly compressed datastore for ticks. It has a couple restrictions by design.

* Uses a simple binary file format: Dense Tick Format(DTF)

* order book tick data tuple (timestamp, seq, is\_trade, is\_bid, price, size).

* Append only for now

* sorted by seq/timestamp

* Stores one row of data in 13 bytes.

* 12.4MB per 1 million row


This software is motivated by reducing expenditure. 500GB stored on Google Cloud PostgreSQL was too expensive. Since financial data is usually read in bulk.