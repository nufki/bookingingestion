package ch.innuvation.bookingingestion.config

import com.avaloq.acp.bde.protobuf.books.Books
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@Configuration
class TestKafkaConfig {

    @Bean
    fun protobufProducerFactory(kafkaProperties: KafkaProperties): ProducerFactory<String, Books> {
        // Use properties from application-test.yaml, but override value serializer for protobuf
        val props = kafkaProperties.buildProducerProperties().toMutableMap()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaProtobufSerializer::class.java
        props["specific.protobuf.value.type"] = Books::class.java.name
        return DefaultKafkaProducerFactory<String, Books>(props)
    }

    @Bean
    fun protobufKafkaTemplate(producerFactory: ProducerFactory<String, Books>): KafkaTemplate<String, Books> {
        return KafkaTemplate(producerFactory)
    }
}

