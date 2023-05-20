package org.example

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.example.consumer.ConsumerWorker
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.Executors

class ConsumerApplication {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val workers = mutableListOf<ConsumerWorker>()

    fun main(args: Array<String>) {
        Runtime.getRuntime().addShutdownHook(ShutDownThread())

        val properties = getProperties()

        val executorService = Executors.newCachedThreadPool()

        for (i in 1..CONSUMER_COUNT) {
            workers.add(ConsumerWorker(properties, TOPIC_NAME, i))
        }
        workers.forEach(executorService::execute)
    }

    inner class ShutDownThread: Thread() {
        override fun run() {
            logger.info("shutdown hook!")
            workers.forEach(ConsumerWorker::stopAndWakeup)
        }
    }

    private fun getProperties(): Properties {
        val configs = Properties()

        configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
        configs[ConsumerConfig.GROUP_ID_CONFIG] = GROUP_ID
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name

        return configs
    }

    companion object {
        const val BOOTSTRAP_SERVERS = "localhost:9092"
        const val TOPIC_NAME = "select-color"
        const val GROUP_ID = "color-hdfs-save-consumer-group"
        const val CONSUMER_COUNT = 3
    }
}
