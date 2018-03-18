const redis = require('redis')
    , Redlock = require('redlock')
    , fs = require('fs')
    , uuidv1 = require('uuid/v1');

const resource = 'master';

class Worker{

    constructor(interval = 500){
        this.ID = uuidv1();
        this.interval = interval;
        this.client = redis.createClient();
        this.masterSelector = redis.createClient();
        this.sender = redis.createClient();
        this.redlock = Worker.getLockerInstance(this.masterSelector);
    }

    start(){
        const {ID, interval, redlock, checkMaster} = this;

        fs.appendFileSync('./startup.log', `Start worker, ${ID} \n`, {flag: 'a'});

        setInterval(() =>
            checkMaster.bind(this)(ID)
                .then( isMaster => isMaster ? this.sendMsg() : this.processMsg())
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
                    return trySetupMaster.bind(self)()
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
//
//
//
//
//
//
//
// const resource = 'master';
// const ID = uuidv1()
//
// let client, client1, sender;
// let redlock;
// // the maximum amount of time you want the resource locked,
// // keeping in mind that you can extend the lock up until
// // the point when it expires
// let ttl = 1000;
// let isGenerator = false;
//
// exports.start = (interval) => {
//     client = redis.createClient();
//     client1 = redis.createClient();
//     sender = redis.createClient();
//     redlock = _getLockerInstance(client1)
//     ttl = interval;
//
//     fs.appendFileSync('./startup.log', `Start worker, ${ID} \n`);
//
//     setInterval(() => {
//         _checkMaster(ID)
//             .then(
//                 isMaster => {
//                     if (isMaster){
//                         _sendMsg();
//                     } else{
//                         _processMsg()
//                     }
//                 }
//             )
//             .catch(
//                 err => console.error('Check master err:', err)
//             )
//     }, interval );
//
//     redlock.on('clientError', function(err) {
//         console.error('A redis error has occurred:', err);
//     });
//
// }
//
// function _processMsg(){
//     // console.log('Process msg by', ID);
//     const timeout = parseInt(ttl/1000) || 1
//     client.blpop('messages', 0, (e, r)=>{
//         if (e) return console.warn('Process error:',e);
//
//         if (r){
//             // console.log(`Message ${r[1]} processed by ${ID}`);
//             Math.random() <= 0.05 && client.rpush('errors', r)
//             fs.appendFileSync('receive', `"${r[1]}",\n`)
//         // } else{
//         //     console.log(`Message empty`);
//         }
//     })
// }
//
// function _sendMsg(){
//     const msg = `${ID}_${Date.now()}`
//     // console.log('Send msg from', msg)
//     sender.rpush('messages', msg)
//     fs.appendFileSync('send', `"${msg}",\n`)
// }
//
// function _trySetupMaster() {
//     console.log('Try setup master');
//     // Simple solution for one instance
//     // return new Promise((res, rej)=> {
//     //     client.set(resource, ID, 'PX', ttl * 2, 'NX', function(err, r) {
//     //         if(err) {
//     //             console.log('Err setup master', err);
//     //             return rej(err)
//     //         }
//     //
//     //         console.log('Success setup master', ID);
//     //         isGenerator = true;
//     //         return res(isGenerator)
//     //     })
//     // })
//     return redlock.lock(resource, ttl)
//         .then((lock)=> {
//             console.log('Success lock');
//
//             client1.set(resource, ID, 'PX', ttl * 2, function(err, res) {
//                 if(err) {
//                     console.log('Err setup master', err);
//                     return lock.unlock()
//                         .catch(function (err) {
//                             // we weren't able to reach redis; your lock will eventually
//                             // expire, but you probably want to log this error
//                             console.warn(err);
//                         });
//                 }
//                 // unlock your resource when you are done
//                 if (res){
//                     console.log('Success setup master', ID);
//                     isGenerator = true;
//                 }
//                 setTimeout(()=>{
//                     process.exit(0)
//                 }, 10000)
//                 return lock.unlock()
//                     .then(r => console.log('Success unlock'))
//                     .catch(function (err) {
//                         // we weren't able to reach redis; your lock will eventually
//                         // expire, but you probably want to log this error
//                         console.warn(err);
//                     });
//             })
//             // if you need more time, you can continue to extend
//             // the lock as long as you never let it expire
//
//             // this will extend the lock so that it expires
//             // approximitely 1s from when `extend` is called
//             // function extend(){
//             //     return lock.extend(ttl).then(function(lock){
//             //         return extend()
//             //         // ...do something here...
//             //
//             //         // unlock your resource when you are done
//             //         // return lock.unlock()
//             //         //     .catch(function(err) {
//             //         //         // we weren't able to reach redis; your lock will eventually
//             //         //         // expire, but you probably want to log this error
//             //         //         console.error(err);
//             //         //     });
//             //     })
//             //
//             // }
//             // return extend()
//         }).catch(function(err) {
//             // we weren't able to reach redis; your lock will eventually
//             // expire, but you probably want to log this error
//             console.error(err);
//         });
// }
//
// function _getLockerInstance(client){
//     return new Redlock(
//         // you should have one client for each independent redis node
//         // or cluster
//         [client],
//         {
//             // the expected clock drift; for more details
//             // see http://redis.io/topics/distlock
//             driftFactor: 0.01, // time in ms
//
//             // the max number of times Redlock will attempt
//             // to lock a resource before erroring
//             retryCount:  2,
//
//             // the time in ms between attempts
//             retryDelay:  200, // time in ms
//
//             // the max time in ms randomly added to retries
//             // to improve performance under high contention
//             // see https://www.awsarchitectureblog.com/2015/03/backoff.html
//             retryJitter:  200 // time in ms
//         }
//     )
// }
//
// function _checkMaster(ID) {
//     // console.log('Check master', ID)
//     return new Promise((res, rej)=>{
//         client1.get(resource, function (err, id) {
//             if (err) {
//                 return rej(err);
//             }
//
//             // console.log('Check master get', id, ID);
//
//             if (!id){
//                 return _trySetupMaster()
//             } else{
//                 if (id === ID) {
//                     client1.pexpire(resource, ttl*2, function (err) {
//                         if (err) {
//                             console.warn('error', err);
//                             return rej(err)
//                         }
//
//                         res(true)
//                     })
//                 } else{
//                     res(false)
//                 }
//             }
//         })
//     })
//
// }