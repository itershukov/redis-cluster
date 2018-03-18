const cluster = require('cluster')
    , fs      = require('fs')
    , Worker  = require('./worker');

require('dotenv').config()

const num = process.env.NUM_INSTANCES || 50;
const interval = process.env.INTERVAL || 500;

if (process.env.NODE_ENV === 'getErrors'){
    return Worker.getErrors();
}

if (cluster.isMaster) {
    console.log(`Master ${process.pid} is running`, num, interval);
    fs.appendFileSync('./startup.log', `Master ${process.pid} is running, ${num}, ${interval} \n`, {flag: 'a'});

    for (let i = 0; i < num; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} died`);
    });
} else {
    console.log(`Start worker`, num, interval);
    // worker.start(interval)
    const instance = new Worker();
    instance.start(interval)
}