package com.example.pipeline

import com.example.pipeline.configure.ElasticSearchSinkConnectorConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.LoggerFactory

class ElasticSearchSinkConnector : SinkConnector() {
    private val logger = LoggerFactory.getLogger(javaClass)
    private lateinit var configProperties: Map<String, String>

    override fun version(): String {
        return "1.0"
    }

    override fun start(props: Map<String, String>) {
        configProperties = props
        try {
            ElasticSearchSinkConnectorConfig(
                props = props
            )
        } catch (e: ConfigException) {
            throw ConnectException(e.message, e)
        }
    }

    override fun taskClass(): Class<out Task> {
        return ElasticSearchSinkTask::class.java
    }

    override fun taskConfigs(maxTasks: Int): MutableList<Map<String, String>> {
        val taskConfigs = mutableListOf<Map<String, String>>()
        val taskProps = mutableMapOf<String, String>()

        taskProps.putAll(configProperties)

        for (i in 0..maxTasks) {
            taskConfigs.add(taskProps)
        }
        return taskConfigs
    }

    override fun config(): ConfigDef {
        return ElasticSearchSinkConnectorConfig.config
    }

    override fun stop() {
        logger.info("Stop elasticsearch connector")
    }
}