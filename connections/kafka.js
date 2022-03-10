const Producer = require('../kafka/SingleProducer');
const Consumer = require('../kafka/SingleConsumer');

const producer = new Producer({ 'metadata.broker.list': 'localhost:9092' })
const consumer = new Consumer({ 'metadata.broker.list': 'localhost:9092' }, {}, {}, 'ORDER_MANAGEMENT_SRV')

module.exports = {
    producer,
    consumer
}
