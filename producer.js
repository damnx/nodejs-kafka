const Kafka = require('node-rdkafka');

const producer = new Kafka.Producer({
    'client.id': 'kafka',
    'metadata.broker.list': 'localhost:9092',
    // 'compression.codec': 'gzip',
    'retry.backoff.ms': 200,
    'message.send.max.retries': 10,
    'socket.keepalive.enable': true,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 1000000,
    'dr_cb': true
});

const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

producer.connect() 



producer.on('ready', () => {
    console.log('rl-----', rl.on)
    rl.on('line', line => {
        console.log("send:", line);
        try {
            producer.produce(
                // Topic to send the message to
                'damn04',
                // optionally we can manually specify a partition for the message
                // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
                null,
                // Message to send. Must be a buffer
                Buffer.from(line),
                // for keyed messages, we also specify the key - note that this field is optional
                null,
                // you can send a timestamp here. If your broker version supports it,
                // it will get added. Otherwise, we default to 0
                Date.now(),
                // you can send an opaque token here, which gets passed along
                // to your delivery reports
            );
        } catch (err) {
            console.error('A problem occurred when sending our message');
            console.error(err);
        }
    })
})