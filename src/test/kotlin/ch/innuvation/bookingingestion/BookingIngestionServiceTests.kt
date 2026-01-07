package ch.innuvation.bookingingestion

import com.avaloq.acp.bde.protobuf.books.Books
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BookingIngestionServiceTests : IntegrationTest() {


    @Test
    fun `ingests protobuf books into Oracle`() {
        val messages = loadBooksMessages()

        bookingIngestionService.ingestBatch(messages)

        val bookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        val evtPktCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0
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