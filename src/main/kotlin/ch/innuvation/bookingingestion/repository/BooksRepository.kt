package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.proto.toLocalDateOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.Books
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Date
import javax.sql.DataSource

@Repository
class BooksRepository(
    dataSource: DataSource
) {
    private val jdbcTemplate = JdbcTemplate(dataSource)
    private val log = logger()

    /**
     * Batch upsert into BOOKS using Oracle MERGE statement.
     * Executes one MERGE per row for batch processing.
     */
    fun upsertBatch(books: List<Books>) {
        if (books.isEmpty()) return

        // Oracle MERGE statement for upsert (one row at a time)
        val sql = """
            MERGE INTO BOOKS b
            USING (
                SELECT ? AS EVT_ID, ? AS BU_ID, ? AS EVT_STATUS_ID, 
                       ? AS VERI_DATE, ? AS BOOK_DATE, ? AS VAL_DATE, 
                       ? AS TRX_DATE, ? AS PERF_DATE
                FROM DUAL
            ) s ON (b.EVT_ID = s.EVT_ID)
            WHEN MATCHED THEN
                UPDATE SET 
                    BU_ID = s.BU_ID,
                    EVT_STATUS_ID = s.EVT_STATUS_ID,
                    VERI_DATE = s.VERI_DATE,
                    BOOK_DATE = s.BOOK_DATE,
                    VAL_DATE = s.VAL_DATE,
                    TRX_DATE = s.TRX_DATE,
                    PERF_DATE = s.PERF_DATE
            WHEN NOT MATCHED THEN
                INSERT (EVT_ID, BU_ID, EVT_STATUS_ID, VERI_DATE, BOOK_DATE, VAL_DATE, TRX_DATE, PERF_DATE)
                VALUES (s.EVT_ID, s.BU_ID, s.EVT_STATUS_ID, s.VERI_DATE, s.BOOK_DATE, s.VAL_DATE, s.TRX_DATE, s.PERF_DATE)
        """.trimIndent()

        try {
            // Execute batch update - Oracle MERGE works one row at a time
            jdbcTemplate.batchUpdate(sql, object : org.springframework.jdbc.core.BatchPreparedStatementSetter {
                override fun setValues(ps: java.sql.PreparedStatement, i: Int) {
                    val book = books[i]
                    val evtId = book.evtId.toLongOrNull()
                        ?: throw IllegalArgumentException("evtId is required")

                    ps.setLong(1, evtId)
                    ps.setObject(2, book.buId.toLongOrNull(), java.sql.Types.BIGINT)
                    ps.setObject(3, book.evtStatusId.toLongOrNull(), java.sql.Types.BIGINT)
                    ps.setObject(4, book.veriDate.toLocalDateOrNull()?.let { Date.valueOf(it) }, java.sql.Types.DATE)
                    ps.setObject(5, book.bookDate.toLocalDateOrNull()?.let { Date.valueOf(it) }, java.sql.Types.DATE)
                    ps.setObject(6, book.valDate.toLocalDateOrNull()?.let { Date.valueOf(it) }, java.sql.Types.DATE)
                    ps.setObject(7, book.trxDate.toLocalDateOrNull()?.let { Date.valueOf(it) }, java.sql.Types.DATE)
                    ps.setObject(8, book.perfDate.toLocalDateOrNull()?.let { Date.valueOf(it) }, java.sql.Types.DATE)
                }

                override fun getBatchSize(): Int = books.size
            })
        } catch (e: DataAccessException) {
            log.error("Failed to upsert BOOKS batch: ${e.message}", e)
            throw e
        }
    }
}
