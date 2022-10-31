const Kafka = require('node-rdkafka')
const { v4: uuidv4 } = require('uuid');

console.log("*** Consumer starts... ***")

const taskConsumer = Kafka.KafkaConsumer({
    'group.id': 'taskConsumers',
    'metadata.broker.list': 'localhost:9092'
}, {});

taskConsumer.connect()

const answerStream = Kafka.Producer.createWriteStream({
    'metadata.broker.list': 'localhost:9092'
}, {}, { topic: 'answer' });

taskConsumer.on('ready', () => {
    console.log('Consumer ready...')
    taskConsumer.subscribe(['task'])
    taskConsumer.consume()
}).on('data', (data) => {

    const obj = JSON.parse(data.value)
    let answer = `Calculation ${obj.o1}+${obj.o2}=${obj.o3} is`
    if (obj.o1 + obj.o2 === obj.o3) {
        answer += " true.";
    } else {
        answer += " FALSE!";
    }

    console.log(answer)

    const success =
        answerStream.write(Buffer.from(answer));

    if (success) {
        console.log(`Answer ${obj.id} successfully to stream`);
    } else {
        console.log('Problem writing to answer stream..');
    }
})