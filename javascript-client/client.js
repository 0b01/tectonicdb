var net = require('net');



class TectonicDB {

    constructor(host, port) {
        this.client = new net.Socket();
        this.client.connect(9001, '127.0.0.1', function() {
            console.log('Connected');
        });
    }

    cmd(command) {
        this.client.write(`${commmand}\n`);
    }

    this.client.on('data', function(data) {
        console.log('Received: ' + typeof data);
        client.destroy();
    });

    client.on('close', function() {
        console.log('Connection closed');
    });
}