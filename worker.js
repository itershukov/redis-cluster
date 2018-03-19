const redis = require('redis')
    , Redlock = require('redlock')
    , fs = require('fs')
    , uuidv1 = require('uuid/v1');

const resource = 'master';

class Worker{

    constructor(interval = 500){
        this.ID = uuidv1();
        this.interval = interval;
    }

    start(){
        this.client = redis.createClient();
        this.masterSelector = redis.createClient();
        this.sender = redis.createClient();
        this.redlock = Worker.getLockerInstance(this.masterSelector);

        const {ID, interval, redlock, checkMaster} = this;

        fs.appendFileSync('./startup.log', `Start worker, ${ID} \n`, {flag: 'a'});

        setInterval(() =>
            checkMaster.call(this, ID)
                .then( isMaster => isMaster ? this.sendMsg() : this.processMsg())
                .catch(err => console.error('Check master err:', err))
        , interval );

        redlock.on('clientError', err => console.error('A redis error has occurred:', err));
    }

    static getErrors(){
        const client = redis.createClient();
        return function readAndClear(){
            // console.log('Start read error:');
            client.lpop('errors', (e, r)=>{
                if (e) return console.warn('Get errors error:', e);
                // console.log('Error read success:', r);
                if (r){
                    console.log(r);
                    readAndClear()
                } else{
                    process.exit(0)
                }
            })
        }()
    }

    processMsg(){
        // console.log('Process msg by', ID);
        const {client, sender} = this;

        client.blpop('messages', 0, (e, r)=>{
            if (e) return console.warn('Process error:',e);

            if (r){
                // console.log(`Message ${r[1]} processed by ${ID}`);
                Math.random() <= 0.05 && sender.rpush('errors', r[1])
                fs.appendFileSync('receive', `"${r[1]}",\n`, {flag: 'a'})
            }
        })
    }

    sendMsg(){
        const {sender, ID} = this;
        const msg = `${ID}_${Date.now()}`
        // console.log('Send msg from', msg)
        sender.rpush('messages', msg)
        fs.appendFileSync('send', `"${msg}",\n`, {flag: 'a'})
    }

    trySetupMaster() {
        console.log('Try setup master');
        const {interval, ID, redlock, masterSelector} = this;

        return redlock.lock(resource, interval)
            .then((lock)=> {
                console.log('Success lock');

                return masterSelector.set(resource, ID, 'PX', interval * 2, function(err, res) {
                    if(err) {
                        console.log('Err setup master', err);
                        return lock.unlock()
                    }

                    if (res){
                        console.log('Success setup master', ID);
                    }
                    ///KILL CURRENT PROCESS
                    setTimeout(()=>{
                        process.exit(0)
                    }, 10000);
                    ////////////////////////////

                    return lock.unlock()

                })
            })
            .then(r => console.log('Success unlock'))
            .catch(e => console.error(e));
    }

    checkMaster() {
        // console.log('Check master', ID)

        const {ID, masterSelector, trySetupMaster, interval} = this
            , self = this;
        return new Promise((res, rej)=>
            masterSelector.get(resource, function (err, id) {
                if (err) return rej(err);

                // console.log('Check master get', id, ID);

                if (!id){
                    return trySetupMaster.call(self)
                }
                if (id === ID) {
                    return masterSelector.pexpire(resource, interval*1.5, function (err) {
                        if (err) {
                            console.warn('error', err);
                            return rej(err)
                        }

                        return res(true)
                    })
                }

                return res(false)
            })
        )
    }

    static getLockerInstance(client){
        return new Redlock(
            // you should have one client for each independent redis node
            // or cluster
            [client],
            {
                // the expected clock drift; for more details
                // see http://redis.io/topics/distlock
                driftFactor: 0.01, // time in ms

                // the max number of times Redlock will attempt
                // to lock a resource before erroring
                retryCount:  2,

                // the time in ms between attempts
                retryDelay:  200, // time in ms

                // the max time in ms randomly added to retries
                // to improve performance under high contention
                // see https://www.awsarchitectureblog.com/2015/03/backoff.html
                retryJitter:  200 // time in ms
            }
        )
    }
}

module.exports = Worker;