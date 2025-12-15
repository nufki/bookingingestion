package ch.innuvation.bookingingestion.config

import org.slf4j.LoggerFactory
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.stereotype.Component

@Component
class KafkaListenerConfigLogger(
    private val registry: KafkaListenerEndpointRegistry
) {

    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener(ContextRefreshedEvent::class)
    fun logKafkaListenerConfig() {
        val container = registry.getListenerContainer("books-listener")
        if (container is ConcurrentMessageListenerContainer<*, *>) {
            log.info("Kafka listener 'books-listener' concurrency = ${container.concurrency}")
        } else {
            log.warn("Kafka listener 'books-listener' container not found or not concurrent")
        }
    }
}
