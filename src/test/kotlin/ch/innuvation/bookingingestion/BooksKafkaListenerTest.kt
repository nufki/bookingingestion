package ch.innuvation.bookingingestion

import ch.innuvation.bookingingestion.proto.toLongOrNull
import com.avaloq.acp.bde.protobuf.books.Books
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.test.utils.ContainerTestUtils
import java.io.File
import java.math.BigDecimal

class BooksKafkaListenerTest : IntegrationTest() {

    @BeforeEach
    fun setUp() {
        // Clean database before each test
        cleanupDatabase()

        // Wait for listener to be assigned partitions
        val listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("books-listener")
        listenerContainer?.let {
            ContainerTestUtils.waitForAssignment(it, 3)
        }
    }

    @Test
    fun `ingests books messages from Kafka into database`() {
        // Load test messages
        val messages = loadBooksMessages()
        assertThat(messages).isNotEmpty

        // Send messages to Kafka
        messages.forEach { book ->
            val evtId = book.evtId.toString()
            sendBooksMessage(evtId, book)
        }

        // Wait for processing (listener processes batches, so give it time)
        Thread.sleep(2000)

        // Assert database state
        val expectedBookCount = messages.size
        val expectedEvtPktCount = messages.sumOf { it.evtPktList.size }

        awaitBooksCount(expectedBookCount)
        awaitEvtPktCount(expectedEvtPktCount)

        // Verify specific records
        messages.forEach { book ->
            val evtId = book.evtId.toLongOrNull()
            assertThat(evtId).isNotNull

            val bookRecord = jdbcTemplate.queryForMap(
                "SELECT * FROM BOOKS WHERE EVT_ID = ?",
                evtId
            )

            assertThat(bookRecord).isNotNull
            // Oracle JDBC returns BigDecimal for numeric columns
            val actualEvtId = (bookRecord["EVT_ID"] as? BigDecimal)?.toLong()
            assertThat(actualEvtId).isEqualTo(evtId)

            // Verify EVT_PKT records for this book
            val evtPktRecords = jdbcTemplate.queryForList(
                "SELECT * FROM EVT_PKT WHERE EVT_ID = ?",
                evtId
            )

            assertThat(evtPktRecords.size).isEqualTo(book.evtPktList.size)
        }
    }

    @Test
    fun `handles batch processing correctly`() {
        // Load all test messages
        val messages = loadBooksMessages()
        assertThat(messages.size).isGreaterThan(1) // Ensure we have multiple messages

        // Send all messages in quick succession
        messages.forEach { book ->
            sendBooksMessage(book.evtId.toString(), book)
        }

        // Wait for batch processing
        Thread.sleep(3000)

        // Assert all were processed
        awaitBooksCount(messages.size)
        awaitEvtPktCount(messages.sumOf { it.evtPktList.size })
    }

    @Test
    fun `handles duplicate messages with upsert`() {
        // Load a single message
        val messages = loadBooksMessages()
        assertThat(messages).isNotEmpty
        val firstMessage = messages.first()

        val evtIdString = firstMessage.evtId.toString()

        // Send message first time
        sendBooksMessage(evtIdString, firstMessage)
        Thread.sleep(1000)

        awaitBooksCount(1)

        // Send same message again (should upsert, not create duplicate)
        sendBooksMessage(evtIdString, firstMessage)
        Thread.sleep(1000)

        // Should still have only one record
        val bookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        assertThat(bookCount).isEqualTo(1)

        // Verify the record exists
        val evtId = firstMessage.evtId.toLongOrNull()
        assertThat(evtId).isNotNull
        val bookRecord = jdbcTemplate.queryForMap(
            "SELECT * FROM BOOKS WHERE EVT_ID = ?",
            evtId
        )

        assertThat(bookRecord).isNotNull
    }

    private fun loadBooksMessages(): List<Books> {
        val resource = ClassPathResource("books")
        val dir = File(resource.uri)
        return dir.listFiles()?.map { file ->
            getProtobufFromJson(file, Books.newBuilder()).build()
        } ?: emptyList()
    }
}