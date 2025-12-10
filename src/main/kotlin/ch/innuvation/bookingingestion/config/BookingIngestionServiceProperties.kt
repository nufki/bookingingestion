package ch.innuvation.bookingingestion.config

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * config-properties
 */
@ConfigurationProperties("innuvation.bookingingestion-service")
data class BookingIngestionServiceProperties(
    val books: TopicNameConfig,
    val listenerAutoStartup: Boolean = true
) {
    data class TopicNameConfig(
        val inputTopicName: String,
    )
}
