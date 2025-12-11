package ch.innuvation.bookingingestion.config

import com.avaloq.acp.bde.protobuf.books.Books
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.AsyncTaskExecutor
import org.springframework.core.task.support.TaskExecutorAdapter
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import java.util.concurrent.Executors

@Configuration
class VirtualThreadConfig {

    @Bean
    fun virtualThreadExecutor(): AsyncTaskExecutor =
        TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor())

    @Bean
    fun kafkaListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, Books>,
        virtualThreadExecutor: AsyncTaskExecutor
    ): ConcurrentKafkaListenerContainerFactory<String, Books> =
        ConcurrentKafkaListenerContainerFactory<String, Books>().apply {
            this.consumerFactory = consumerFactory
            this.containerProperties.listenerTaskExecutor = virtualThreadExecutor
        }
}

