import {TectonicDB, TectonicPool} from '../src/';
const process = require('process');

const db = new TectonicPool();

async function test() {
    let pong = await db.ping();
    console.log(pong);
    process.exit()
}

test();