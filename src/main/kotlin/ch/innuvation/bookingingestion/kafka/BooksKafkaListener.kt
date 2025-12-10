package ch.innuvation.bookingingestion.kafka

import ch.innuvation.bookingingestion.service.BookingIngestionService
import com.avaloq.acp.bde.protobuf.books.Books
import kotlinx.coroutines.runBlocking
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
        batch = "true"
    )
    fun consumeBooks(records: List<ConsumerRecord<String, Books?>>) = runBlocking {
        log.info("Received batch of ${records.size} Books messages")

        val messages = records
            .mapNotNull { it.value() }   // no tombstones, but just to be safe

        try {
            bookingIngestionService.ingestBatch(messages)
        } catch (e: Exception) {
            log.error("Error ingesting batch of Books; size=${messages.size}", e)
            // optional: DLQ / retry
        }
    }
}
