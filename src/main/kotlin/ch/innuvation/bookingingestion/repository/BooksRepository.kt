package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.proto.toLocalDateOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.Books
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.springframework.stereotype.Repository
import java.math.BigInteger

@Repository
class BooksRepository(
    private val jooq: DSLContext,
) {

    private val log = logger()

    /**
     * Batch upsert into BOOKS using jOOQ's onDuplicateKeyUpdate().
     * For MySQL: translates to INSERT ... ON DUPLICATE KEY UPDATE
     * For Oracle: translates to MERGE statement
     */
    fun upsertBatch(books: List<Books>) {
        if (books.isEmpty()) return

        val insert = jooq.insertInto(
            BOOKS,
            BOOKS.EVT_ID,
            BOOKS.BU_ID,
            BOOKS.EVT_STATUS_ID,
            BOOKS.VERI_DATE,
            BOOKS.BOOK_DATE,
            BOOKS.VAL_DATE,
            BOOKS.TRX_DATE,
            BOOKS.PERF_DATE
        ).apply {
            books.forEach { msg ->
                val evtId = msg.evtId.toLongOrNull()
                    ?: throw IllegalArgumentException("evtId is required")

                values(
                    BigInteger.valueOf(evtId),
                    msg.buId.toLongOrNull()?.let { BigInteger.valueOf(it) },
                    msg.evtStatusId.toLongOrNull()?.let { BigInteger.valueOf(it) },
                    msg.veriDate.toLocalDateOrNull(),
                    msg.bookDate.toLocalDateOrNull(),
                    msg.valDate.toLocalDateOrNull(),
                    msg.trxDate.toLocalDateOrNull(),
                    msg.perfDate.toLocalDateOrNull()
                )
            }
        }
            .onDuplicateKeyUpdate()
            .setAllToExcluded()   // use EXCLUDED.* values for update

        try {
            insert.execute()
        } catch (e: DataAccessException) {
            log.error("Failed to upsert BOOKS batch: ${e.message}", e)
            throw e
        }
    }
}
