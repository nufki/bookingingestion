package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.exception.DatabaseExceptionHandler
import ch.innuvation.bookingingestion.proto.toLocalDateOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.Books
import oracle.jdbc.OracleConnection
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Date
import javax.sql.DataSource

@Repository
class BooksRepository(
    private val dataSource: DataSource
) {
    private val jdbcTemplate = JdbcTemplate(dataSource)
    private val log = logger()

    /**
     * Batch upsert into BOOKS using PL/SQL FORALL with bulk collect.
     */
    fun upsertBatch(books: List<Books>) {
        if (books.isEmpty()) return

        try {
            dataSource.connection.use { conn ->
                val oracleConn = conn.unwrap(OracleConnection::class.java)

                // Prepare arrays for PL/SQL
                val evtIds = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    books.map { it.evtId.toLongOrNull() }.toTypedArray())
                val buIds = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    books.map { it.buId.toLongOrNull() }.toTypedArray())
                val evtStatusIds = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    books.map { it.evtStatusId.toLongOrNull() }.toTypedArray())
                val veriDates = oracleConn.createARRAY("SYS.ODCIDATELIST",
                    books.map { it.veriDate.toLocalDateOrNull()?.let { d -> Date.valueOf(d) } }.toTypedArray())
                val bookDates = oracleConn.createARRAY("SYS.ODCIDATELIST",
                    books.map { it.bookDate.toLocalDateOrNull()?.let { d -> Date.valueOf(d) } }.toTypedArray())
                val valDates = oracleConn.createARRAY("SYS.ODCIDATELIST",
                    books.map { it.valDate.toLocalDateOrNull()?.let { d -> Date.valueOf(d) } }.toTypedArray())
                val trxDates = oracleConn.createARRAY("SYS.ODCIDATELIST",
                    books.map { it.trxDate.toLocalDateOrNull()?.let { d -> Date.valueOf(d) } }.toTypedArray())
                val perfDates = oracleConn.createARRAY("SYS.ODCIDATELIST",
                    books.map { it.perfDate.toLocalDateOrNull()?.let { d -> Date.valueOf(d) } }.toTypedArray())

                // Call stored procedure
                conn.prepareCall("{call upsert_books_bulk(?, ?, ?, ?, ?, ?, ?, ?)}").use { stmt ->
                    stmt.setArray(1, evtIds)
                    stmt.setArray(2, buIds)
                    stmt.setArray(3, evtStatusIds)
                    stmt.setArray(4, veriDates)
                    stmt.setArray(5, bookDates)
                    stmt.setArray(6, valDates)
                    stmt.setArray(7, trxDates)
                    stmt.setArray(8, perfDates)
                    stmt.execute()
                }
            }
        } catch (e: Exception) {
            DatabaseExceptionHandler.handleBulkUpsertFailure("BOOKS", e, log)
        }
    }

    /**
     * Batch delete using PL/SQL FORALL with bulk collect.
     */
    fun deleteBatch(evtIds: List<Long>) {
        if (evtIds.isEmpty()) return

        try {
            dataSource.connection.use { conn ->
                val oracleConn = conn.unwrap(OracleConnection::class.java)

                // Prepare array for PL/SQL
                val evtIdsArray = oracleConn.createARRAY("SYS.ODCINUMBERLIST", evtIds.toTypedArray())

                // Call stored procedure
                conn.prepareCall("{call delete_books_bulk(?)}").use { stmt ->
                    stmt.setArray(1, evtIdsArray)
                    stmt.execute()
                }
            }
        } catch (e: Exception) {
            DatabaseExceptionHandler.handleBulkUpsertFailure("BOOKS", e, log)
        }
    }
}