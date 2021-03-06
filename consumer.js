const Kafka = require('node-rdkafka');

const consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
    'partition.assignment.strategy': "roundrobin" // tham số này để đia các mess theo vòng tròn trong tập hợp group.id
})

consumer.connect()
consumer.on('ready', () => {
    consumer.subscribe(['damn04']);
    consumer.consume()
}).on('data', data => {
    console.log(data.value.toString());
})