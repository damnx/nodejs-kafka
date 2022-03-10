const Kafka = require('node-rdkafka');

const client = Kafka.AdminClient.create({
    'client.id': 'kafka-admin',
    'metadata.broker.list': 'localhost:9092',
});

client.createTopic({
    topic: 'damnx',
    num_partitions: 12,
    replication_factor: 1
}, function (err) {
    // Done!
    console.log('err----', err)
});