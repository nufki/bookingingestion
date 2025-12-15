package ch.innuvation.bookingingestion.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin.NewTopics

@Configuration
@Profile("test")
class KafkaConfigTest {

    @Bean
    fun inputTopic(
        properties: BookingIngestionServiceProperties
    ) =
        NewTopics(
            TopicBuilder.name(properties.books.inputTopicName)
                .partitions(3)
                .build(),
        )


}
