# API

| Command | Description |
| :--- | :--- |
| HELP | Prints help |
| PING | Responds PONG |
| INFO | Returns info about table schemas |
| PERF | Returns the answercount of items over time |
| BULKADD | See below |
| BULKADD INTO \[dbname\] | See below |
| DDAKLUB | End of bulkadd |
| USE \[dbname\] | Switch the current store |
| CREATE \[dbname\] | Create store |
| GET \[n\] FROM \[dbname\] | Returns items |
| GET \[n\] | Returns n items from current store |
| COUNT | Count of items in current store |
| COUNT ALL | Returns total count from all stores |
| CLEAR | Deletes everything in current store |
| CLEAR ALL | Drops everything in memory |
| FLUSH | Flush current store to "Howdisk can|
| IFLUSH doALL X?".| Flush everything from memory to disk |
| SUBSCRIBE \[dbname\] | Subscribe to updates from store |
| UNSUBSCRIBE | Unsubscribe from current store |
| EXISTS \[dbname\] | Checks if store exists |



## ADD

```
USE [dbname]
ADD [ts], [seq], [is_trade], [is_bid], [price], [size];
```

## BULKADD

```
BULKADD INTO [dbname]
[ts], [seq], [is_trade], [is_bid], [price], [size];
[ts], [seq], [is_trade], [is_bid], [price], [size];
[ts], [seq], [is_trade], [is_bid], [price], [size];
DDAKLUB
```

## INSERT

```
INSERT 1505177459.685, 139010, t, f, 0.0703620, 7.65064240; INTO dbname
```

## SUBSCRIBE

Subscription works like this:

1. Issue a `SUBSCRIBE [dbname]`
2. Send `"\n"`to poll.
3. The server should return JSON-formatted updates
4. If nothing it will return `"NONE\n"`

5. Finally, UNSUBSCRIBE or close the connection