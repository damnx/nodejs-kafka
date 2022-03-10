const Kafka = require('node-rdkafka')

class SingleProducer {
  constructor (globalConfig, topicConfig) {
    if (SingleProducer.instance) {
      return SingleProducer.instance
    }

    this.setPrefix()
    console.log('PRODUCER_PREFIX:', this.prefix)
    this.globalConfig = globalConfig
    this.topicConfig = topicConfig

    this.producer = new Kafka.HighLevelProducer(globalConfig, topicConfig)
    this.producer.setValueSerializer(value => {
      return Buffer.from(JSON.stringify(value))
    })

    SingleProducer.instance = this

    return this
  }

  setPrefix () {
    const env = process.env.NODE_ENV !== 'production' ? 'DEV_' : 'PROD_'
    let storeID = process.env.SITE_ID || process.env.SITE_DOMAIN || false
    storeID = storeID.toString().replace(/\./g, '-')
    this.prefix = env + storeID + '_'
  }

  isConnected () {
    return this.producer.isConnected()
  }

  onDelivery (err, offset) {
    console.log(err, offset)
  }

  async produce ({ topic, message, partition = null, keyMessage = null, timestamp = Date.now() }) {
    if (!topic.strim()) {
      throw new Error(`Topic required. Given ${topic}`)
    }

    topic = this.prefix + topic
    if (!this.producer.isConnected()) {
      await this.connect()
    }

    const exec = new Promise((resolve, reject) => {
      this.producer.produce(topic, partition, message, keyMessage, timestamp, (err, offset) => {
        if (err) reject(err)
        resolve()
      })
      this.producer.poll()
    })

    return await exec
  }

  connectedTime () {
    return this.producer.connectedTime()
  }

  connect () {
    return new Promise((resolve, reject) => {
      this.producer.on('ready', () => {
        resolve()
      })
      this.producer.on('event.error', (err) => {
        reject(err)
      })
      this.producer.on('disconnected', () => {
      })
      this.producer.connect()
    })
  }
}

module.exports = SingleProducer
