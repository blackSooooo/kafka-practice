package com.example.pipeline.configure

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef

class ElasticSearchSinkConnectorConfig(
    props: Map<String, String>
): AbstractConfig(config, props) {

    companion object {
        const val ES_CLUSTER_HOST = "es.host"
        const val ES_CLUSTER_HOST_DEFAULT_VALUE = "localhost"
        const val ES_CLUSTER_HOST_DOC = "input es host"

        const val ES_CLUSTER_PORT = "es.port"
        const val ES_CLUSTER_PORT_DEFAULT_VALUE = "9200"
        const val ES_CLUSTER_PORT_DOC = "input es port"

        const val ES_INDEX = "es.index"
        const val ES_INDEX_DEFAULT_VALUE = "kafka-connector-index"
        const val ES_INDEX_DOC = "input es index"
        val config: ConfigDef = ConfigDef()
            .define(ES_CLUSTER_HOST, ConfigDef.Type.STRING, ES_CLUSTER_HOST_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_CLUSTER_HOST_DOC)
            .define(ES_CLUSTER_PORT, ConfigDef.Type.STRING, ES_CLUSTER_PORT_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_CLUSTER_PORT_DOC)
            .define(ES_INDEX, ConfigDef.Type.STRING, ES_INDEX_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_INDEX_DOC)
    }
}