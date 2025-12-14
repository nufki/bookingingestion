package ch.innuvation.bookingingestion.config

import com.avaloq.acp.bde.protobuf.books.Books
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.core.task.support.TaskExecutorAdapter
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import java.util.concurrent.Executors

@Configuration
class VirtualThreadConfig(
    private val kafkaProperties: KafkaProperties,
    private val bookingIngestionServiceProperties: BookingIngestionServiceProperties
) {

    @Bean
    fun virtualThreadExecutor(): AsyncTaskExecutor =
        TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor())

    @Bean
    @Primary
    @ConditionalOnExpression("'${'$'}{spring.kafka.bootstrap-servers:}'.length() > 0")
    fun consumerFactoryWithBatchSize(): ConsumerFactory<String, Books> {
        val props = kafkaProperties.buildConsumerProperties().toMutableMap()
        // Override max.poll.records with configured batch size
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = bookingIngestionServiceProperties.batchSize
        return DefaultKafkaConsumerFactory<String, Books>(props)
    }

    @Bean
    @ConditionalOnBean(ConsumerFactory::class) // conditionally define the bean only if other Kafka beans exist because of the test
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Books>,
        virtualThreadExecutor: AsyncTaskExecutor
    ): ConcurrentKafkaListenerContainerFactory<String, Books> =
        ConcurrentKafkaListenerContainerFactory<String, Books>().apply {
            this.consumerFactory = consumerFactory
            this.containerProperties.listenerTaskExecutor = virtualThreadExecutor
        }
}

