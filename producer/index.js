const Kafka = require('node-rdkafka')
const { v4: uuidv4 } = require('uuid');

console.log("*** Producer starts... ***")

const answerConsumer = Kafka.KafkaConsumer({
    'group.id': 'answerConsumers',
    'metadata.broker.list': 'localhost:9092'
}, {});

answerConsumer.connect();

answerConsumer.on('ready', () => {
    console.log('consumer ready...');
    answerConsumer.subscribe(['answer']);
    answerConsumer.consume();
}).on('data', (data) => {
    console.log(`received message: ${data.value}`);
});

const taskStream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, { topic: 'task' });   

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from + 1))) + from
}

 function queueMessage() {
    const o1 = randomizeIntegerBetween(1, 2)
    const o2 = randomizeIntegerBetween(1, 2)
    const o3 = randomizeIntegerBetween(1, 2)
    const id = uuidv4().substring(30)

    let obj = { o1, o2, o3, id };
    let objJSON = JSON.stringify(obj);

    const success = taskStream.write(Buffer.from(
        `{"o1":${o1}, "o2":${o2}, "o3":${o3}, "id":"${id}"}`
    ))

    if (success) {
        console.log(`Task ${id} succesfully to stream`)
    } else {
        console.log('Problem writing to stream..')
    }
}

setInterval( () => {
    queueMessage();
}, 2500);
    