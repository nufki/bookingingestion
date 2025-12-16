package ch.innuvation.bookingingestion

import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import ch.innuvation.bookingingestion.service.BookingIngestionService
import com.avaloq.acp.bde.protobuf.books.Books
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import org.testcontainers.containers.OracleContainer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(
    classes = [BookingIngestionApplication::class],
    properties = [
        "spring.kafka.bootstrap-servers="  // Empty = no Kafka broker
    ]
)
@TestPropertySource(properties = ["innuvation.bookingingestion-service.listener-auto-startup=false"])
class BookingIngestionServiceTests @Autowired constructor(
    private val bookingIngestionService: BookingIngestionService,
    private val dsl: DSLContext
) {

    companion object {
        @Container
        private val oracle: OracleContainer = OracleContainer(DockerImageName.parse("gvenzl/oracle-xe:latest"))
            .withDatabaseName("BOOKING_INGESTION_DB")
            .withUsername("books")
            .withPassword("books")
            .withReuse(true)

        @JvmStatic
        @BeforeAll
        fun initContainer() {
            // ensures container is started before tests run
            if (!oracle.isRunning) {
                oracle.start()
            }
        }

        @JvmStatic
        @DynamicPropertySource
        fun registerProps(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { oracle.jdbcUrl }
            registry.add("spring.datasource.username") { oracle.username }
            registry.add("spring.datasource.password") { oracle.password }
            registry.add("spring.flyway.url") { oracle.jdbcUrl }
            registry.add("spring.flyway.user") { oracle.username }
            registry.add("spring.flyway.password") { oracle.password }
            registry.add("spring.kafka.listener.auto-startup") { false }
        }
    }

    @Test
    fun `ingests protobuf books into Oracle`() {
        val messages = loadBooksMessages()

        bookingIngestionService.ingestBatch(messages)

        val bookCount = dsl.fetchCount(BOOKS)
        val evtPktCount = dsl.fetchCount(EVT_PKT)
        val expectedEvtPkts = messages.sumOf { it.evtPktList.size }

        assertEquals(messages.size, bookCount, "book rows")
        assertEquals(expectedEvtPkts, evtPktCount, "evt_pkt rows")
    }

    private fun loadBooksMessages(): List<Books> =
        this::class.java.classLoader.getResource("books")!!
            .let { resource ->
                val dir = java.io.File(resource.toURI())
                dir.listFiles()?.map { file ->
                    getProtobufFromJson(file, Books.newBuilder()).build()
                } ?: emptyList()
            }

}