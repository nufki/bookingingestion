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
import org.testcontainers.containers.MySQLContainer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource

class BookingIngestionServiceTests : IntegrationTest() {


    @Test
    fun `ingests protobuf books into MySQL`() {
        val messages = loadBooksMessages()

        bookingIngestionService.ingestBatch(messages)

        val bookCount = jooq.fetchCount(BOOKS)
        val evtPktCount = jooq.fetchCount(EVT_PKT)
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