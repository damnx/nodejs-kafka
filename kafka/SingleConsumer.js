const Kafka = require('node-rdkafka')

class SingleConsumer {
  constructor (globalConfig, topicConfig = {}, topics, handler) {
    if (SingleConsumer.instance) {
      return SingleConsumer.instance
    }

    this.setPrefix()
    console.log('CONSUMER_PREFIX:', this.prefix)
    this.checkHandler(handler)
    this.setTopics(topics)
    this.handler = handler
    SingleConsumer.instance = this
    this.consumer = new Kafka.KafkaConsumer(globalConfig, topicConfig)

    return this
  }

  setPrefix () {
    const env = process.env.NODE_ENV !== 'production' ? 'DEV_' : 'PROD_'
    let storeID = process.env.SITE_ID || process.env.SITE_DOMAIN || false
    storeID = storeID.toString().replace(/\./g, '-')
    this.prefix = env + storeID + '_'
  }

  setTopics (topics) {
    this.checkTopics(topics)
    for (const i in topics) {
      topics[i] = this.prefix + topics[i]
    }

    this.topics = topics
  }

  checkTopics (topics) {
    if (!Array.isArray(topics)) {
      throw new TypeError(`Topics must be an array, ${typeof topics} given`)
    }

    if (topics.length === 0) {
      throw new Error('Topics is empty')
    }
  }

  checkHandler (handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('Handler is not a function!')
    }
  }

  onReady (context) {
    context.consumer.subscribe(context.topics)
    context.consumer.consume()
  }

  onDisconnect () {

  }

  onError (error) {
    console.log(error)
  }

  onData (data, context) {
    context.checkHandler(context.handler)
    context.handler(data)
  }

  consume () {
    const that = this
    this.checkTopics(this.topics)
    this.checkHandler(this.handler)
    this.consumer.on('ready', () => this.onReady(this))
    this.consumer.on('disconnected', that.onDisconnect)
    this.consumer.on('event.error', that.onError)
    this.consumer.on('data', data => this.onData(data, this))

    this.consumer.connect()
  }

  isConnected () {
    return this.consumer.isConnected()
  }

  commitMessage (message) {
    return this.consumer.commitMessage(message)
  }

  commit (topicPartition) {
    return this.consumer.commit(topicPartition)
  }
}

module.exports = SingleConsumer
