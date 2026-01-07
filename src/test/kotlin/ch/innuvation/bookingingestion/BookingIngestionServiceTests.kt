package ch.innuvation.bookingingestion

import com.avaloq.acp.bde.protobuf.Wrappers
import com.avaloq.acp.bde.protobuf.books.Books
import com.google.protobuf.Int64Value
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class BookingIngestionServiceTests : IntegrationTest() {

    @BeforeEach
    fun setUp() {
        // Clear tables before each test
        jdbcTemplate.execute("DELETE FROM EVT_PKT")
        jdbcTemplate.execute("DELETE FROM BOOKS")
    }

    @Test
    fun `ingests protobuf books into Oracle`() {
        val messages = loadBooksMessages()

        bookingIngestionService.ingestBatch(messages, emptyList())

        val bookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        val evtPktCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0
        val expectedEvtPkts = messages.sumOf { it.evtPktList.size }

        assertEquals(messages.size, bookCount, "book rows")
        assertEquals(expectedEvtPkts, evtPktCount, "evt_pkt rows")
    }

    @Test
    fun `handles tombstones and deletes books with cascading EVT_PKT`() {
        // First, insert some books
        val messages = loadBooksMessages()
        bookingIngestionService.ingestBatch(messages, emptyList())

        val initialBookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        val initialEvtPktCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0

        // Now send tombstones for some of the books
        val evtIdsToDelete = messages.take(2).map { it.evtId }
        val tombstones = evtIdsToDelete.map { it.value.toString() }

        bookingIngestionService.ingestBatch(emptyList(), tombstones)

        // Verify deletions
        val finalBookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        val finalEvtPktCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0

        val deletedEvtPkts = messages.take(2).sumOf { it.evtPktList.size }

        assertEquals(initialBookCount - 2, finalBookCount, "books should be deleted")
        assertEquals(initialEvtPktCount - deletedEvtPkts, finalEvtPktCount, "EVT_PKT rows should cascade delete")
    }

    @Test
    fun `handles mixed batch with inserts, updates, and tombstones`() {
        // Insert initial data
        val messages = loadBooksMessages()
        bookingIngestionService.ingestBatch(messages, emptyList())

        // Prepare mixed batch: update first book, delete second, insert new one
        val updatedBook = messages.first()
            .toBuilder()
            .setBuId(Wrappers.SInt64Value.newBuilder().setValue(999))
            .build()

        val newBook = Books.newBuilder()
            .setEvtId(Wrappers.SInt64Value.newBuilder().setValue(999999))
            .setBuId(Wrappers.SInt64Value.newBuilder().setValue(123))
            .build()

        val tombstone = messages[1].evtId.value.toString()

        bookingIngestionService.ingestBatch(
            listOf(updatedBook, newBook),
            listOf(tombstone)
        )

        // Verify results
        val bookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        assertEquals(messages.size, bookCount, "total books: deleted 1, added 1")

        // Verify the update
        val updatedBuId = jdbcTemplate.queryForObject(
            "SELECT BU_ID FROM BOOKS WHERE EVT_ID = ?",
            Long::class.java,
            updatedBook.evtId.value
        )
        assertEquals(999L, updatedBuId, "BU_ID should be updated")

        // Verify the deletion
        val deletedCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM BOOKS WHERE EVT_ID = ?",
            Int::class.java,
            messages[1].evtId.value
        ) ?: 0
        assertEquals(0, deletedCount, "deleted book should not exist")

        // Verify new insert
        val newBookExists = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM BOOKS WHERE EVT_ID = ?",
            Int::class.java,
            999999L
        ) ?: 0
        assertEquals(1, newBookExists, "new book should be inserted")
    }

    @Test
    fun `handles empty tombstone list`() {
        val messages = loadBooksMessages()

        // Should work fine with empty tombstone list
        bookingIngestionService.ingestBatch(messages, emptyList())

        val bookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        assertEquals(messages.size, bookCount)
    }

    @Test
    fun `handles empty message list with tombstones`() {
        // Insert some data first
        val messages = loadBooksMessages()
        bookingIngestionService.ingestBatch(messages, emptyList())

        // Send only tombstones
        val tombstones = messages.map { it.evtId.value.toString() }
        bookingIngestionService.ingestBatch(emptyList(), tombstones)

        val bookCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM BOOKS", Int::class.java) ?: 0
        val evtPktCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM EVT_PKT", Int::class.java) ?: 0

        assertEquals(0, bookCount, "all books should be deleted")
        assertEquals(0, evtPktCount, "all EVT_PKT rows should cascade delete")
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