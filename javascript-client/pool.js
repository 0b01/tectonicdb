const TectonicDB = require('./tectonic');

const THREADS = 20;
const PORT = 9001;
const HOST = '0.0.0.0';

class TectonicPool {
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
        return new TectonicDB(this.port, this.address, this.onDisconnect);
    }

    onDisconnect(queue) {
        // this.sockets = this.sockets.map((socket) => socket.dead ? this.newSocket() : socket);
        // this.bestSocket().concatQueue(queue);
    }

    bestSocket(){
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

    async add(update) {
        return this.bestSocket().add(update);
    }

    async bulkadd(updates) {
        return this.bestSocket().bulkadd(updates);
    }

    async bulkadd_into(updates, db) {
        this.bestSocket().bulkadd_into(updates, db);
    }

    async insert(update, db) {
        this.bestSocket().insert(update, db);
    }

    async getall() {
        return this.bestSocket().getall();
    }

    async get(n) {
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

    async create(dbname) {
        return this.bestSocket().create(dbname);
    }

    async use(dbname) {
        return this.bestSocket().use(dbname);
    }

    async exists(dbname) {
        return this.bestSocket().exists(dbname);
    }

    async exit() {
        await Promise.all(this.sockets.map((db) => db.exit()));
    }
}

module.exports = TectonicPool;