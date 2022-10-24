const Kafka = require('node-rdkafka')
console.log("*** Producer starts... ***")

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list':'localhost:9092'
}, {}, {topic: 'task'})

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to-from+1))) + from
}

function queueMessage() {
    const o1 = randomizeIntegerBetween(1,2)
    const o2 = randomizeIntegerBetween(1,2)
    const o3 = randomizeIntegerBetween(1,2)

    const success = stream.write(Buffer.from(
        `Is this correct ${o1} + ${o2} = ${o3}?`
    ))

    if(success) {
        console.log('message succesfully to stream')
    }else {
        console.log('Problem writing to stream..')
    }
}

setInterval( () => {
    queueMessage()
}, 2500)