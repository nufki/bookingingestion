package ch.innuvation.bookingingestion

import ch.innuvation.bookingingestion.config.BookingIngestionServiceProperties
import ch.innuvation.bookingingestion.config.TestcontainerTestExecutionListener
import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import ch.innuvation.bookingingestion.kafka.BooksKafkaListener
import ch.innuvation.bookingingestion.service.BookingIngestionService
import ch.innuvation.bookingingestion.utils.Profiles
import com.avaloq.acp.bde.protobuf.books.Books
import org.assertj.core.api.Assertions.assertThat
import org.jooq.DSLContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.context.event.ApplicationEventsTestExecutionListener

@TestExecutionListeners(
    listeners = [
        TestcontainerTestExecutionListener::class,
        ApplicationEventsTestExecutionListener::class,
    ],
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS,
)
@EmbeddedKafka(
    kraft = true,
    brokerProperties = ["auto.create.topics.enable=false"]
)
@ActiveProfiles(Profiles.TEST)
@SpringBootTest
abstract class IntegrationTest {

    @Autowired
    @Qualifier("protobufKafkaTemplate")
    protected lateinit var kafkaTemplate: KafkaTemplate<String, Books>

    @Autowired
    protected lateinit var kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry

    @Autowired
    protected lateinit var jooq: DSLContext

    @Autowired
    protected lateinit var properties: BookingIngestionServiceProperties

    @Autowired
    protected lateinit var booksKafkaListener: BooksKafkaListener

    @Autowired
    protected lateinit var bookingIngestionService: BookingIngestionService

    protected fun sendBooksMessage(key: String, message: Books) {
        kafkaTemplate.send(properties.books.inputTopicName, key, message).get()
    }

    protected fun awaitBooksCount(expectedCount: Int, timeoutSeconds: Long = 10) {
        val startTime = System.currentTimeMillis()
        while (System.currentTimeMillis() - startTime < timeoutSeconds * 1000) {
            val count = jooq.fetchCount(BOOKS)
            if (count == expectedCount) {
                return
            }
            Thread.sleep(100)
        }
        val actualCount = jooq.fetchCount(BOOKS)
        assertThat(actualCount).`as`("Expected $expectedCount books but found $actualCount").isEqualTo(expectedCount)
    }

    protected fun awaitEvtPktCount(expectedCount: Int, timeoutSeconds: Long = 10) {
        val startTime = System.currentTimeMillis()
        while (System.currentTimeMillis() - startTime < timeoutSeconds * 1000) {
            val count = jooq.fetchCount(EVT_PKT)
            if (count == expectedCount) {
                return
            }
            Thread.sleep(100)
        }
        val actualCount = jooq.fetchCount(EVT_PKT)
        assertThat(actualCount).`as`("Expected $expectedCount evt_pkt rows but found $actualCount").isEqualTo(expectedCount)
    }

    protected fun cleanupDatabase() {
        // Delete in order to respect foreign key constraints (if any)
        jooq.deleteFrom(EVT_PKT).execute()
        jooq.deleteFrom(BOOKS).execute()
    }
}

