package ch.innuvation.bookingingestion.kafka

import ch.innuvation.bookingingestion.config.KafkaConfig.Companion.BOOKING_INGESTION_SERVICE_CONTAINER_FACTORY_BEAN_NAME
import ch.innuvation.bookingingestion.service.BookingIngestionService
import com.avaloq.acp.bde.protobuf.books.Books
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class BooksKafkaListener(
    private val bookingIngestionService: BookingIngestionService
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(
        id = "books-listener",
        topics = ["\${innuvation.bookingingestion-service.books.input-topic-name}"],
        groupId = "\${spring.kafka.consumer.group-id}",
        batch = "true",
        containerFactory = BOOKING_INGESTION_SERVICE_CONTAINER_FACTORY_BEAN_NAME
    )
    fun consumeBooks(records: List<ConsumerRecord<String, Books?>>) {
        log.info("Received batch of ${records.size} Books messages")

        // Separate tombstones from actual messages
        val tombstones = records.filter { it.value() == null }.map { it.key() }
        val messages = records.mapNotNull { it.value() }

        try {
            bookingIngestionService.ingestBatch(messages, tombstones)
        } catch (e: Exception) {
            log.error("Error ingesting batch: messages=${messages.size}, tombstones=${tombstones.size}", e)
            throw e
        }
    }
}