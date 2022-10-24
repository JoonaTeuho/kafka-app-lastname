const Kafka = require('node-rdkafka')

console.log("*** Consumer starts... ***")

const consumer = Kafka.KafkaConsumer({
    'group.id':'kafka',
    'metadata.broker.list':'localhost:9092'
}, {})

consumer.connect()

consumer.on('ready', () => {
    console.log('Consumer ready...')
    consumer.subscribe(['task'])
    consumer.consume()
}).on('data', (data) => {
    console.log(`received message: ${data.value}`)
})