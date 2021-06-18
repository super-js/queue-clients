const net = require('net');
//
// const server = net.createServer();
//
// // Grab an arbitrary unused port.
// server.listen({
//     host: '0.0.0.0',
//     port: 8888
// }, () => {
//     console.log('listening');
// });
//
// server.on('error', err => console.error(err));
// server.on('data', buf => console.log(buf))
// server.on('connection', ev => console.log(`New connection ${ev}`))
const socket = net.connect({
    host: '192.168.1.11',
    port: 4554
});

socket.on('connect', ev => console.log(`Connected, ${ev}`))