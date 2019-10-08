# Javascript ingestor

```javascript
// tslint:disable-next-line:no-var-requires
// import bittrex from './bittrex';
const bittrex = require('./node.bittrex.api');

import {
    DBUpdate,
    ExchangeCallback,

    ExchangeState,
    ExchangeStateUpdate,

    PairUpdate,
    SummaryCallback,

    SummaryState,
} from './typings';

import db from './db';

import { toPair } from './utils';

function allMarkets() : Promise<[string]> {
    return new Promise((resolve, reject) => {
        bittrex.getmarketsummaries( ( data : any, err : never) => {
            if (err) { reject(err); }
            const ret = data.result.map((market : PairUpdate) => market.MarketName);
            resolve(ret);
        });
    });
}

function formatUpdate(v : ExchangeStateUpdate) {
    const updates : DBUpdate[] = [];

    const pair = toPair(v.MarketName);
    const seq = v.Nounce;
    const timestamp = Date.now();

    v.Buys.forEach((buy) => {
        updates.push(
            {
                pair,
                seq,
                is_trade: false,
                is_bid: true,
                price: buy.Rate,
                size: buy.Quantity,
                timestamp,
                type: buy.Type,
            },
        );
    });

    v.Sells.forEach((sell) => {
        updates.push(
            {
                pair,
                seq,
                is_trade: false,
                is_bid: false,
                price: sell.Rate,
                size: sell.Quantity,
                timestamp,
                type: sell.Type,
            },
        );
    });

    v.Fills.forEach((fill) => {
        updates.push(
            {
                pair,
                seq,
                is_trade: true,
                is_bid: fill.OrderType === 'BUY',
                price: fill.Rate,
                size: fill.Quantity,
                timestamp: (new Date(fill.TimeStamp)).getTime(),
                type: null,
            },
        );
    });

    return updates;
}

function listen(markets : string[], exchangeCallback?: ExchangeCallback, summaryCallback?: SummaryCallback) : void {
    const websocketsclient = bittrex.websockets.subscribe(markets, (data : ExchangeState | SummaryState ) => {
        if (data.M === 'updateExchangeState') {
            data.A.forEach(exchangeCallback);
        } else if (data.M === 'updateSummaryState') {
            data.A[0].Deltas.forEach(summaryCallback);
        } else {
            console.log('--------------',data); // <never>
        }
    });
}

async function initTables(markets : string[]) {
    const pairs = markets.map(toPair);

    const create = await Promise.all(
        pairs.map((pair) => new Promise(async (resolve, reject) => {
            const exists = (await db.exists(pair)).success;
            if (!exists) {
                console.log(`${pair} table does not exist. Creating...`);
                await db.create(pair);
            }
            resolve(true);
        })),
    );

    console.log('Double checking...');
    const created = await Promise.all(pairs.map((pair) =>
        new Promise(async (resolve, reject) => {
            const {success} = await db.exists(pair);
            resolve(success);
        })));
    for (let i = 0; i < created.length; i++) {
        if (!created[i]) {
            throw new Error(`Table for '${pairs[i]}' cannot be created.`);
        }
    }
}

async function watch() {
    try {
        // const mkts = ['BTC-NEO', 'BTC-ETH'];
        const mkts = await allMarkets();
        await initTables(mkts.map(m => "bt_"+m));
        console.log('Tables created.');
        listen(mkts, (v) => {
            const updates : DBUpdate[] = formatUpdate(v);
            const pair = updates[0].pair;
            for (const up of updates){
                db.insert(up, "bt_"+pair);
            }
        });
    } catch (e) {
        console.log(e);
        throw e;
    }
}

const main = watch;

main();
```

###### Note: these example programs are not maintained anymore, especially this one. The entire market data to Rust is ported to Rust now.