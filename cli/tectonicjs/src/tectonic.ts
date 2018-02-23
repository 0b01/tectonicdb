const net = require('net');
const textEncoding = require('text-encoding');
const THREADS = 20;
const PORT = 9001;
const HOST = 'localhost';

const TextDecoder = textEncoding.TextDecoder;

import { DBUpdate } from './typings';

export interface TectonicResponse {
    success: boolean;
    data: string;
}

export type SocketMsgCb = (res: TectonicResponse) => void;

export interface SocketQuery {
    message: string;
    cb: SocketMsgCb;
    onError: (err: any) => void;
}

export default class TectonicDB {
    port : number;
    address : string;
    socket: any;
    initialized: boolean;
    dead: boolean;
    private onDisconnect: any;

    private socketSendQueue: SocketQuery[];
    private activeQuery?: SocketQuery | null;
    private readerBuffer: Buffer;

    // tslint:disable-next-line:no-empty
    constructor(port=PORT, address=HOST, onDisconnect=((queue: SocketQuery[]) => { })) {
        this.socket = new net.Socket();
        this.activeQuery = null;
        this.address = address || HOST;
        this.port = port || PORT;
        this.initialized = false;
        this.dead = false;
        this.onDisconnect = onDisconnect;
        this.init();
    }

    async init() {
        const client = this;

        client.socketSendQueue = [];
        client.readerBuffer = new Buffer([]);

        client.socket.connect(client.port, client.address, () => {
            // console.log(`Tectonic client connected to: ${client.address}:${client.port}`);
            this.initialized = true;

            // process any queued queries
            if(this.socketSendQueue.length > 0) {
                // console.log('Sending queued message after DB connected...');
                client.activeQuery = this.socketSendQueue.shift();
                if (this.activeQuery != null)
                    client.sendSocketMsg(this.activeQuery.message);
            }
        });

        client.socket.on('close', () => {
            client.dead = true;
            client.onDisconnect(this.socketSendQueue);
        });

        client.socket.on('data', (data: any) =>
            this.handleSocketData(data));

        client.socket.on('error', (err: any) => {
            if(client.activeQuery) {
                client.activeQuery.onError(err);
            }
        });
    }

    async info() {
        return this.cmd('INFO');
    }

    async ping() {
        return this.cmd('PING');
    }

    async help() {
        return this.cmd('HELP');
    }

    async add(update : DBUpdate) {
        const { timestamp, seq, is_trade, is_bid, price, size } = update;
        return this.cmd(`ADD ${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
    }

    async bulkadd(updates : DBUpdate[]) {
        const ret = [];
        ret.push('BULKADD');
        for (const { timestamp, seq, is_trade, is_bid, price, size} of updates) {
            ret.push(`${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
        }
        ret.push('DDAKLUB');
        this.cmd(ret);
    }

    async bulkadd_into(updates : DBUpdate[], db: string) {
        const ret = [];
        ret.push('BULKADD INTO '+ db);
        for (const { timestamp, seq, is_trade, is_bid, price, size} of updates) {
            ret.push(`${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size};`);
        }
        ret.push('DDAKLUB');
        this.cmd(ret);
    }

    async insert(update: DBUpdate, db : string) {
        const { timestamp, seq, is_trade, is_bid, price, size } = update;
        return this.cmd(`ADD ${timestamp}, ${seq}, ${is_trade ? 't' : 'f'}, ${is_bid ? 't':'f'}, ${price}, ${size}; INTO ${db}`);
    }

    async getall() {
        const {success, data} = await this.cmd('GET ALL AS JSON');
        if (success) {
            return JSON.parse(data);
        } else {
            return null;
        }
    }

    async get(n : number) {
        const {success, data} = await this.cmd(`GET ${n} AS JSON`);
        if (success) {
            return JSON.parse(data);
        } else {
            return data;
        }
    }

    async clear() {
        return this.cmd('CLEAR');
    }

    async clearall() {
        return this.cmd('CLEAR ALL');
    }

    async flush() {
        return this.cmd('FLUSH');
    }

    async flushall(): Promise<TectonicResponse> {
        return this.cmd('FLUSH ALL');
    }

    async create(dbname: string): Promise<TectonicResponse> {
        return this.cmd(`CREATE ${dbname}`);
    }

    async use(dbname: string) {
        return this.cmd(`USE ${dbname}`);
    }

    exists(dbname: string) {
        return this.cmd(`EXISTS ${dbname}`);
    }


    handleSocketData(data: Buffer) {
        const client = this;

        const totalLength = client.readerBuffer.length + data.length;
        client.readerBuffer = Buffer.concat([client.readerBuffer, data], totalLength);

        // check if received a full response from stream, if no, store to buffer.
        const firstResponse = client.readerBuffer.indexOf(0x0a); // chr(0x0a) == '\n'
        if (firstResponse === -1) { // newline not found
            return;
        } else {
            // data up to first newline
            const data = client.readerBuffer.subarray(0, firstResponse+1);
            // remove up to first newline
            const rest = client.readerBuffer.subarray(firstResponse+1, client.readerBuffer.length);
            client.readerBuffer = new Buffer(rest);

            const success = data.subarray(0, 8)[0] === 1;
            // const len = new Uint32Array(data.subarray(8,9))[0];
            const dataBody = new TextDecoder("utf-8").decode(data.subarray(9));
            const response = {success, data: dataBody};

            if (client.activeQuery) {
                // execute the stored callback with the result of the query, fulfilling the promise
                client.activeQuery.cb(response);
            }

            // if there's something left in the queue to process, do it next
            // otherwise set the current query to empty
            if(client.socketSendQueue.length === 0) {
                client.activeQuery = null;
            } else {
                // equivalent to `popFront()`
                client.activeQuery = this.socketSendQueue.shift();
                if (client.activeQuery)
                    client.sendSocketMsg(client.activeQuery.message);
            }
        }
    }

    sendSocketMsg(msg: string) {
        this.socket.write(msg+'\n');
    }

    cmd(message: string | string[]) : Promise<TectonicResponse> {
        const client = this;
        let ret : any /* Promise<TectonicResponse> */= null;

        if (Array.isArray(message)) {
             ret = new Promise((resolve, reject) => {
                for (const m of message) {
                    client.socketSendQueue.push({
                        message: m,
                        cb: m === 'DDAKLUB' ? resolve : () => {},
                        onError: reject,
                    });
                }
            });
        } else if (typeof message === 'string') {
            ret = new Promise((resolve, reject) => {
                const query: SocketQuery = {
                    message,
                    cb: resolve,
                    onError: reject,
                };
                client.socketSendQueue.push(query);
            });
        }

        if (client.activeQuery == null && this.initialized) {
            client.activeQuery = this.socketSendQueue.shift();
            if (client.activeQuery != null)
                client.sendSocketMsg(client.activeQuery.message);
        }

        return ret;
    }

    exit() {
        this.socket.destroy();
    }

    getQueueLen(): number {
        return this.socketSendQueue.length;
    }

    concatQueue(otherQueue: SocketQuery[]) {
        this.socketSendQueue = this.socketSendQueue
                                .concat(otherQueue);
    }
}
