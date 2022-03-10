const Kafka = require('node-rdkafka');

const client = Kafka.AdminClient.create({
    'client.id': 'kafka-admin',
    'metadata.broker.list': 'localhost:9092',
});

client.createTopic({
    topic: 'damn04',
    num_partitions: 12,
    replication_factor: 1
}, function (err) {
    // Done!
    console.log('err----', err)
});

//khi scale woker thì phải chú ý: số woker làm việc tối đa đồng thời bằng số num_partitions 