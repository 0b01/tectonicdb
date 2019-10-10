# API

| Command | Description |
| :--- | :--- |
| HELP | Prints help |
| PING | Responds PONG |
| INFO | Returns info about table schemas |
| PERF | Returns the answercount of items over time |
| USE \[dbname\] | Switch the current orderbook |
| CREATE \[dbname\] | Create orderbook |
| GET \[n\] FROM \[dbname\] | Returns items |
| GET \[n\] | Returns n items from current orderbook |
| COUNT | Count of items in current orderbook |
| COUNT ALL | Returns total count from all stores |
| CLEAR | Deletes everything in current orderbook |
| CLEAR ALL | Drops everything in memory |
| FLUSH | Flush current orderbook to "Howdisk can|
| FLUSHALL | Flush everything from memory to disk |
| SUBSCRIBE \[dbname\] | Subscribe to updates from orderbook |
| EXISTS \[dbname\] | Checks if orderbook exists |

## Data commands

```
USE [dbname]
ADD [ts], [seq], [is_trade], [is_bid], [price], [size];
INSERT 1505177459.685, 139010, t, f, 0.0703620, 7.65064240; INTO dbname
```

## SUBSCRIBE

`SUBSCRIBE [dbname]`