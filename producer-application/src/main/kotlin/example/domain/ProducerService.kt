package example.domain

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service

@Service
class ProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun send(topic: String, message: String) {
        logger.info("send message > $message")
        kafkaTemplate.send(topic, message).whenComplete { result, e ->
            when (e) {
                null -> handleSuccess(result)
                else -> handleFailure(e)
            }
        }
    }

    private fun handleSuccess(result: SendResult<String, String>) {
        logger.info("[success] $result")
    }

    private fun handleFailure(e: Throwable) {
        logger.info("[failure] ${e.message}, $e")
    }
}