const Kafka = require('node-rdkafka');

const consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka2',
    'metadata.broker.list': 'localhost:9092',
    'partition.assignment.strategy': "range"
})

consumer.connect()
consumer.on('ready', () => {
    consumer.subscribe(['damnxx']);
    consumer.consume()
}).on('data', data => {
    console.log(data.value.toString());
})