package org.example.consumer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class ConsumerWorker(
    private val props: Properties,
    private val topic: String,
    private val threadName: String,
    private val consumer: KafkaConsumer<String, String> = KafkaConsumer(props)
): Runnable {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val bufferString = ConcurrentHashMap<Int, MutableList<String>>()
    private val currentFileOffset = ConcurrentHashMap<Int, Long>()

    constructor(props: Properties, topic: String, number: Int): this(
        props = props,
        topic = topic,
        threadName = "consumer-thread-$number"
    )

    override fun run() {
        Thread.currentThread().name = threadName
        consumer.subscribe(listOf(topic))
        try {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))

                for (record in records) {
                    addHdfsFileBuffer(record)
                }
                saveBufferToHdfsFile(consumer.assignment())
            }
        } catch (e: WakeupException) {
            logger.info("Wakeup consumer!!")
        } catch (e: Exception) {
            logger.info("${e.message}", e)
        } finally {
            consumer.close()
        }
    }

    private fun addHdfsFileBuffer(record: ConsumerRecord<String, String>) {
        val buffer = bufferString.getOrDefault(record.partition(), mutableListOf())
        buffer.add(record.value())
        bufferString[record.partition()] = buffer

        if (buffer.size == 1) {
            currentFileOffset[record.partition()] = record.offset()
        }
    }

    private fun saveBufferToHdfsFile(partitions: Set<TopicPartition>) {
        partitions.forEach{
            checkFlushCount(it.partition())
        }
    }

    private fun checkFlushCount(partition: Int) {
        if (bufferString[partition] != null) {
            if (bufferString[partition]!!.size > FLUSH_RECORD_COUNT - 1) {
                save(partition)
            }
        }
    }

    private fun save(partition: Int) {
        if (bufferString[partition]!!.isNotEmpty()) {
            try {
                val fileName = "/data/color-$partition-${currentFileOffset[partition]}.log"
                val config = Configuration()
                config.set("fs.defaultFS", "hdfs://localhost:9000")
                val hdfsFileSystem = FileSystem.get(config)
                val fileOutputStream = hdfsFileSystem.create(Path(fileName))
                fileOutputStream.writeBytes(StringUtils.join(bufferString[partition], "\n"))
                fileOutputStream.close()

                bufferString[partition] = mutableListOf()
            } catch (e: Exception) {
                logger.info("${e.message}", e)
            }
        }
    }

    fun stopAndWakeup() {
        logger.info("stopAndWakeup!!!")
        consumer.wakeup()
        saveRemainBufferToHdfsFile()
    }

    private fun saveRemainBufferToHdfsFile() {
        bufferString.forEach{ (partition, _) ->
            this.save(partition)
        }
    }

    companion object {
        const val FLUSH_RECORD_COUNT = 10;
    }
}