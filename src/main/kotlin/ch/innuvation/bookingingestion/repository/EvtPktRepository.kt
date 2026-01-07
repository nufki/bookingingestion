package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.exception.DatabaseExceptionHandler
import ch.innuvation.bookingingestion.proto.toBigDecimalOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.proto.toStringOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.EvtPkt
import oracle.jdbc.OracleConnection
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class EvtPktRepository(
    private val dataSource: DataSource
) {
    private val jdbcTemplate = JdbcTemplate(dataSource)
    private val log = logger()

    /**
     * Batch upsert all EVT_PKT rows using PL/SQL FORALL with bulk collect.
     */
    fun upsertBatch(packets: List<Pair<Long, EvtPkt>>) {
        if (packets.isEmpty()) return

        try {
            dataSource.connection.use { conn ->
                val oracleConn = conn.unwrap(OracleConnection::class.java)

                // Prepare arrays for PL/SQL
                val evtIds = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    packets.map { it.first }.toTypedArray())
                val pktSeqNrs = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    packets.map { it.second.pktSeqNr.toLongOrNull() }.toTypedArray())
                val posIds = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    packets.map { it.second.posId.toLongOrNull() }.toTypedArray())
                val qtys = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    packets.map { it.second.qty.toBigDecimalOrNull() }.toTypedArray())
                val qty3s = oracleConn.createARRAY("SYS.ODCINUMBERLIST",
                    packets.map { it.second.qty3.toBigDecimalOrNull() }.toTypedArray())
                val extlBookTexts = oracleConn.createARRAY("SYS.ODCIVARCHAR2LIST",
                    packets.map { it.second.extlBookText.toStringOrNull() }.toTypedArray())

                // Call stored procedure
                conn.prepareCall("{call upsert_evt_pkt_bulk(?, ?, ?, ?, ?, ?)}").use { stmt ->
                    stmt.setArray(1, evtIds)
                    stmt.setArray(2, pktSeqNrs)
                    stmt.setArray(3, posIds)
                    stmt.setArray(4, qtys)
                    stmt.setArray(5, qty3s)
                    stmt.setArray(6, extlBookTexts)
                    stmt.execute()
                }
            }
        } catch (e: Exception) {
            DatabaseExceptionHandler.handleBulkUpsertFailure("EVT_PKT", e, log)
        }
    }
}