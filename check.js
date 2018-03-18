const {readFileSync} = require('fs')

require('dotenv').config()

const getTime = el => el ? el.split('_')[1] : null
    , getPID  = el => el ? el.split('_')[0] : null
    , getData = fileName => JSON.parse(`[${readFileSync(`./${fileName}`, 'utf8').slice(0, -2)}]`)

const send = getData('send')
const receive = getData('receive')

const interval = process.env.INTERVAL || 500;

let errors = 0;
let warnings = 0;

if (send.length !== receive.length){
    errors++;
    console.error('Error processing', send.length, receive.length)
}


send.forEach((el, i)=>{
    if (i){
        const t1 = getTime(el)
            , t2 = getTime(send[i-1])
            , delta = t1 - t2 - interval;

        if (Math.abs(delta)>10){
            const p1 = getPID(send[i-2])
                , p2 = getPID(send[i-1])
                , p3 = getPID(send[i])
                , p4 = getPID(send[i+1])
            if (p1 === p2 && p3 === p4){
                // console.log('Probably restart detected', el, send[i-1], delta)
            } else{
                warnings++;
                console.error('Error(warning) time', el, send[i-1], delta, p1, p2, p3, p4)
            }
        }
    }
    if (!receive.includes(send[i])){
        errors++;
        console.error('Error processing', send[i])
    }
})

console.log(`Finished with:\n ${errors} errors\n ${warnings} warnings`)