import { DBUpdate } from './typings';
import TectonicDB, { SocketQuery, TectonicResponse } from './tectonic';

const THREADS = 20;
const PORT = 9001;
const HOST = '0.0.0.0';

export default class TectonicPool {
    threads: number;
    port : number;
    address : string;
    sockets: TectonicDB[];
    count: number;

    constructor(threads=THREADS, port=PORT, address=HOST) {
        this.port = port;
        this.address = address;
        this.threads = threads;
        this.sockets = [];
        this.count = 0;
        for (let i = 0; i < this.threads; i++) {
            this.sockets.push(this.newSocket());
        }
    }

    newSocket() {
        return new TectonicDB(this.port, this.address, (e) => this.onDisconnect(e));
    }

    onDisconnect(queue: SocketQuery[]) {
        this.sockets = this.sockets.map((socket) => socket.dead ? this.newSocket() : socket);
        this.bestSocket().concatQueue(queue);
    }

    bestSocket(): TectonicDB {
        const lens = this.sockets.map((sock) => sock.getQueueLen());
        this.count++;
        if (this.count % 100) {
            console.log(lens.reduce((acc,i) => acc+i, 0));
        }
        const j = lens.indexOf(Math.min(...lens));
        return this.sockets[j];
    }

    async info() {
        return this.bestSocket().info();
    }

    async ping() {
        return this.bestSocket().ping();
    }

    async help() {
        return this.bestSocket().help();
    }

    async add(update : DBUpdate) {
        return this.bestSocket().add(update);
    }

    async insert(update: DBUpdate, db : string) {
        this.bestSocket().insert(update, db);
    }

    async getall() {
        return this.bestSocket().getall();
    }

    async get(n : number) {
        return this.bestSocket().get(n);
    }

    async clear() {
        return this.bestSocket().clear();
    }

    async clearall() {
        return this.bestSocket().clearall();
    }

    async flush() {
        return this.bestSocket().flush();
    }

    async flushall() {
        return this.bestSocket().flushall();
    }

    async create(dbname: string) {
        return this.bestSocket().create(dbname);
    }

    async use(dbname: string) {
        return this.bestSocket().use(dbname);
    }

    async exists(dbname: string) {
        return this.bestSocket().exists(dbname);
    }

    async exit() {
        await Promise.all(this.sockets.map((db) => db.exit()));
    }
}
