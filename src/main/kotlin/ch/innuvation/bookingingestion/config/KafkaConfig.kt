package ch.innuvation.bookingingestion.config

import com.avaloq.acp.bde.protobuf.books.Books
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory

@Configuration
class KafkaConfig (
    private val kafkaProperties: KafkaProperties,
    private val bookingIngestionServiceProperties: BookingIngestionServiceProperties
) {
    companion object {
        const val BOOKING_INGESTION_SERVICE_CONTAINER_FACTORY_BEAN_NAME = "bookingIngestionServiceContainerFactory"
    }

    /**
     * This one is not required. Springboot Boot wires its default executors (like SimpleAsyncTaskExecutor)
     * to use virtual threads under the hood when spring.threads.virtual.enabled=true.
     * (Use the auto-configured kafkaListenerContainerFactory)
     */
    @Bean(BOOKING_INGESTION_SERVICE_CONTAINER_FACTORY_BEAN_NAME)
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Books>,
    ): ConcurrentKafkaListenerContainerFactory<String, Books> =
        ConcurrentKafkaListenerContainerFactory<String, Books>().apply {
            isBatchListener = true
            this.consumerFactory = consumerFactory
            this.containerProperties.listenerTaskExecutor = SimpleAsyncTaskExecutor().apply { setVirtualThreads(true) }
        }

    @Bean
    fun consumerFactoryWithBatchSize(): ConsumerFactory<String, Books> {
        val props = kafkaProperties.buildConsumerProperties().toMutableMap()
        // Override max.poll.records with configured batch size
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = bookingIngestionServiceProperties.batchSize
        return DefaultKafkaConsumerFactory<String, Books>(props)
    }
}
