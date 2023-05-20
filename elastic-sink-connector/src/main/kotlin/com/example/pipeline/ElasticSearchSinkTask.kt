package com.example.pipeline

import com.example.pipeline.configure.ElasticSearchSinkConnectorConfig
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.Exception

class ElasticSearchSinkTask(
    private var config: ElasticSearchSinkConnectorConfig,
    private var esClient: RestHighLevelClient,
    private val objectMapper: ObjectMapper
): SinkTask() {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun version(): String {
        return "1.0"
    }

    override fun start(props: MutableMap<String, String>) {
        try {
            config = ElasticSearchSinkConnectorConfig(
                props = props
            )
        } catch (e: ConfigException) {
            throw ConnectException(e.message, e)
        }

        esClient = RestHighLevelClient(
            RestClient.builder(
                HttpHost(config.getString(ElasticSearchSinkConnectorConfig.ES_CLUSTER_HOST), config.getInt(ElasticSearchSinkConnectorConfig.ES_CLUSTER_PORT))
            )
        )
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        val bulkRequest = BulkRequest()
        if (records.isNotEmpty()) {
            for (record in records) {
                val mapped = objectMapper.readValue(record.value().toString(), Map::class.java)

                bulkRequest.add(
                    IndexRequest(config.getString(ElasticSearchSinkConnectorConfig.ES_INDEX))
                        .source(mapped, XContentType.JSON)
                )
                logger.info("record: ${record.value()}")
            }
        }

        esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, Listener())
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
        logger.info("flush")
    }

    override fun stop() {
        try {
            esClient.close()
        } catch (e: IOException) {
            logger.info("${e.message}", e)
        }
    }

    inner class Listener: ActionListener<BulkResponse> {
        override fun onResponse(response: BulkResponse) {
            if (response.hasFailures()) {
                logger.info(response.buildFailureMessage())
            } else {
                logger.info("bulk save success")
            }
        }

        override fun onFailure(e: Exception) {
            logger.info("${e.message}", e)
        }
    }
}