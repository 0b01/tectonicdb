const net = require('net');
const PORT = 9001;
const HOST = 'localhost';

import { DBUpdate } from './typings';

interface TectonicResponse {
    success: boolean,
    data: string
}

class TectonicDB {
    port : number;
    address : string;
    socket : any;
    constructor(port=PORT, address=HOST) {
        this.socket = new net.Socket();
        this.address = address || HOST;
        this.port = port || PORT;
        this.init();
    }

    init() {
        var client = this;
        client.socket.connect(client.port, client.address, () => {
            console.log(`Client connected to: ${client.address}:${client.port}`);
        });

        client.socket.on('close', () => {
            console.log('Client closed');
        });
    }

    async info() {
        return await this.cmd("INFO");
    }

    async ping() {

        return await this.cmd("PING");
    }
    
    async help() {
        await this.cmd("HELP");
    }

    async insert(update : DBUpdate, db: string) {
        let { timestamp, seq, is_trade, is_bid, price, size } = update;
        return await this.cmd(`ADD ${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size}; INTO ${db}`);
    }

    async add(update : DBUpdate) {
        let { timestamp, seq, is_trade, is_bid, price, size } = update;
        return await this.cmd(`ADD ${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
    }

    async bulkadd(updates : DBUpdate[]) {
        await this.cmd("BULKADD");
        for (let { timestamp, seq, is_trade, is_bid, price, size} of updates) {
            await this.cmd(`${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
        }
        return await this.cmd("DDAKLUB");
    }

    async getall() {
        let {success, data} = await this.cmd("GET ALL AS JSON")
        console.log(data);
        if (success) {
            return JSON.parse(data);
        } else {
            return null;
        }
    }

    async get(n : number) {
        let {success, data} = await this.cmd(`GET ${n} AS JSON`);
        if (success) {
            return JSON.parse(data);
        }
        else {
            return data;
        }
    }

    async clear() {
        return await this.cmd("CLEAR");
    }

    async clearall() {
        return await this.cmd("CLEAR ALL");
    }

    async flush() {
        return await this.cmd("FLUSH");
    }

    async flushall() {
        return await this.cmd("FLUSH ALL");
    }

    async create(dbname: string) {
        return await this.cmd(`CREATE ${dbname}`);
    }

    async use(dbname: string) {
        return await this.cmd(`USE ${dbname}`);
    }

    cmd(message: string) : Promise<TectonicResponse> {
        var client = this;
        return new Promise((resolve, reject) => {
            client.socket.write(message+"\n");
            client.socket.on('data', (data : any) => {
                let success = data.subarray(0, 8)[0] == 1;
                let len = new Uint32Array(data.subarray(8,9))[0];
                let dataBody : string = String.fromCharCode.apply(null, data.subarray(9, len+12));
                let response : TectonicResponse = {success, data: dataBody};
                resolve(response);
                if (data.toString().endsWith('exit')) {
                    client.exit();
                }
            });
            client.socket.on('error', (err: never) => {
                reject(err);
            });

        });
    }

    exit() {
        this.socket.destroy();
    }
}

export default TectonicDB;