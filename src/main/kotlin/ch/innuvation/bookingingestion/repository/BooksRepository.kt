package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.proto.toLocalDateOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.Books
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.springframework.stereotype.Repository
import reactor.kotlin.core.publisher.toMono

@Repository
class BooksRepository(
    private val jooq: DSLContext,
) {

    private val log = logger()

    /**
     * Batch upsert into BOOKS using one multi-values INSERT ... ON DUPLICATE KEY UPDATE.
     */
    suspend fun upsertBatch(books: List<Books>) {
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
                    evtId,
                    msg.buId.toLongOrNull(),
                    msg.evtStatusId.toLongOrNull(),
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
            .toMono()

        insert
            .doOnError { e -> log.error("Failed to upsert BOOKS batch: ${getError(e)}", e) }
            .awaitSingle()
    }

    private fun getError(t: Throwable): String? {
        return if (t is DataAccessException) {
            val cause = t.getCause(R2dbcException::class.java)
            cause?.message ?: t.message
        } else t.message
    }
}
