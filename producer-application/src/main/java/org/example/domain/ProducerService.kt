package org.example.domain

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class ProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun send(topic: String, message: String) {
        logger.info("send message > $message")
        kafkaTemplate.send(topic, message)
    }
}