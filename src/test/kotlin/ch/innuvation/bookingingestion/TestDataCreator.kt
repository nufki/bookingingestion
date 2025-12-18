package ch.innuvation.bookingingestion

import com.avaloq.acp.bde.protobuf.books.Books
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.core.io.ClassPathResource
import java.util.*


fun main() {
    val producer = createBooksProducer()

    ClassPathResource("books").file.listFiles()!!.forEach {
        val bookMsg = getProtobufFromJson(it, Books.newBuilder()).build()

        val record = ProducerRecord(
            // IMPORTANT: use the same topic your consumer listens to
            "com.avaloq.acp.bde.books",
            bookMsg.evtId.toString(),
            bookMsg,  // NOT bytes; actual Books message
        )

        producer.send(record).get()
        println("Sent book evtId=${bookMsg.evtId}")
    }

    producer.flush()
    producer.close()
}

private fun createBooksProducer(): KafkaProducer<String, Books> {
    val props = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ProducerConfig.CLIENT_ID_CONFIG, "local-books-producer")
        put("security.protocol", "SASL_PLAINTEXT")
        put("sasl.mechanism", "SCRAM-SHA-512")
        put(
            "sasl.jaas.config",
            """org.apache.kafka.common.security.scram.ScramLoginModule required username="client" password="password";"""
        )

        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer::class.java.name)

        // match whatever your schema registry is for dev
        put("schema.registry.url", "http://localhost:9010/apis/ccompat/v7")

        // optional, but fine if you know the concrete type
        put("auto.register.schemas", true)
        put("specific.protobuf.value.type", Books::class.java.name)
    }

    return KafkaProducer(props)
}

/*
private fun <T> getKafkaProducer(valueSerializer: String? = null): KafkaProducer<String, T> {
    val config = Properties()
    config["client.id"] = "localhost"
    config["bootstrap.servers"] = "localhost:9092"
    config["security.protocol"] = "SASL_PLAINTEXT"
    config["sasl.mechanism"] = "SCRAM-SHA-512"
    config["sasl.jaas.config"] =
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"client\" password=\"password\";"
    config["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
    config["value.serializer"] = valueSerializer ?: "org.apache.kafka.common.serialization.ByteArraySerializer"
    config["schema.registry.url"] = "http://localhost:9010/apis/ccompat/v7"
    return KafkaProducer(config)
}
*/
