package ch.innuvation.bookingingestion

import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import ch.innuvation.bookingingestion.proto.toLongOrNull
import com.avaloq.acp.bde.protobuf.books.Books
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.core.io.ClassPathResource
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.test.utils.ContainerTestUtils
import java.io.File
import java.math.BigInteger

class BooksKafkaListenerTest : IntegrationTest() {

    @BeforeEach
    fun setUp() {
        // Clean database before each test
        cleanupDatabase()
        
        // Wait for listener to be assigned partitions
        val listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("books-listener")
        listenerContainer?.let {
            ContainerTestUtils.waitForAssignment(it, 1)
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

            val evtIdBigInt = BigInteger.valueOf(evtId!!)
            val bookRecord = jooq.selectFrom(BOOKS)
                .where(BOOKS.EVT_ID.eq(evtIdBigInt))
                .fetchOne()

            assertThat(bookRecord).isNotNull
            assertThat(bookRecord!![BOOKS.EVT_ID] as? BigInteger).isEqualTo(evtIdBigInt)

            // Verify EVT_PKT records for this book
            val evtPktRecords = jooq.selectFrom(EVT_PKT)
                .where(EVT_PKT.EVT_ID.eq(evtIdBigInt))
                .fetch()

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

        val evtId = firstMessage.evtId.toString()

        // Send message first time
        sendBooksMessage(evtId, firstMessage)
        Thread.sleep(1000)

        awaitBooksCount(1)

        // Send same message again (should upsert, not create duplicate)
        sendBooksMessage(evtId, firstMessage)
        Thread.sleep(1000)

        // Should still have only one record
        val bookCount = jooq.fetchCount(BOOKS)
        assertThat(bookCount).isEqualTo(1)

        // Verify the record exists
        val evtIdLong = firstMessage.evtId.toLongOrNull()
        assertThat(evtIdLong).isNotNull
        val bookRecord = jooq.selectFrom(BOOKS)
            .where(BOOKS.EVT_ID.eq(BigInteger.valueOf(evtIdLong!!)))
            .fetchOne()

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

