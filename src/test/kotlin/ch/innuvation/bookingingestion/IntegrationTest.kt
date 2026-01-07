package ch.innuvation.bookingingestion

import ch.innuvation.bookingingestion.config.BookingIngestionServiceProperties
import ch.innuvation.bookingingestion.config.TestcontainerTestExecutionListener
import ch.innuvation.bookingingestion.kafka.BooksKafkaListener
import ch.innuvation.bookingingestion.service.BookingIngestionService
import ch.innuvation.bookingingestion.utils.Profiles
import com.avaloq.acp.bde.protobuf.books.Books
import org.assertj.core.api.Assertions.assertThat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import javax.sql.DataSource
import org.slf4j.LoggerFactory
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.context.event.ApplicationEventsTestExecutionListener

private val log = LoggerFactory.getLogger("oracle-tc")


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
    protected lateinit var dataSource: DataSource

    protected val jdbcTemplate: JdbcTemplate by lazy { JdbcTemplate(dataSource) }

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
            val count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
            if (count == expectedCount) {
                return
            }
            Thread.sleep(100)
        }
        val actualCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        assertThat(actualCount).`as`("Expected $expectedCount books but found $actualCount").isEqualTo(expectedCount)
    }

    protected fun awaitEvtPktCount(expectedCount: Int, timeoutSeconds: Long = 10) {
        val startTime = System.currentTimeMillis()
        while (System.currentTimeMillis() - startTime < timeoutSeconds * 1000) {
            val count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0
            if (count == expectedCount) {
                return
            }
            Thread.sleep(100)
        }
        val actualCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0
        assertThat(actualCount).`as`("Expected $expectedCount evt_pkt rows but found $actualCount").isEqualTo(expectedCount)
    }

    protected fun cleanupDatabase() {
        // Delete in order to respect foreign key constraints (if any)
        jdbcTemplate.update("DELETE FROM EVT_PKT")
        jdbcTemplate.update("DELETE FROM BOOKS")
    }
}
