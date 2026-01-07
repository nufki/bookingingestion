package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.proto.toBigDecimalOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.proto.toStringOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.EvtPkt
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.sql.PreparedStatement
import javax.sql.DataSource

@Repository
class EvtPktRepository(
    dataSource: DataSource
) {
    private val jdbcTemplate = JdbcTemplate(dataSource)
    private val log = logger()

    /**
     * Batch insert/upsert all EVT_PKT rows for a batch of events.
     * Param is a list of (evtId, pkt) pairs.
     * Uses Oracle MERGE statement with unique constraint on (EVT_ID, PKT_SEQ_NR).
     */
    fun upsertBatch(packets: List<Pair<Long, EvtPkt>>) {
        if (packets.isEmpty()) return

        // Oracle MERGE statement for upsert based on unique constraint (EVT_ID, PKT_SEQ_NR)
        val sql = """
            MERGE INTO EVT_PKT e
            USING (
                SELECT ? AS EVT_ID, ? AS PKT_SEQ_NR, ? AS POS_ID, 
                       ? AS QTY, ? AS QTY3, ? AS EXTL_BOOK_TEXT
                FROM DUAL
            ) s ON (e.EVT_ID = s.EVT_ID AND e.PKT_SEQ_NR = s.PKT_SEQ_NR)
            WHEN MATCHED THEN
                UPDATE SET 
                    POS_ID = s.POS_ID,
                    QTY = s.QTY,
                    QTY3 = s.QTY3,
                    EXTL_BOOK_TEXT = s.EXTL_BOOK_TEXT
            WHEN NOT MATCHED THEN
                INSERT (EVT_ID, PKT_SEQ_NR, POS_ID, QTY, QTY3, EXTL_BOOK_TEXT)
                VALUES (s.EVT_ID, s.PKT_SEQ_NR, s.POS_ID, s.QTY, s.QTY3, s.EXTL_BOOK_TEXT)
        """.trimIndent()

        try {
            // Execute batch update - Oracle MERGE works one row at a time
            jdbcTemplate.batchUpdate(sql, object : org.springframework.jdbc.core.BatchPreparedStatementSetter {
                override fun setValues(ps: java.sql.PreparedStatement, i: Int) {
                    val (evtId, pkt) = packets[i]
                    val pktSeqNr = pkt.pktSeqNr.toLongOrNull()
                        ?: throw IllegalArgumentException("pktSeqNr is required")

                    ps.setLong(1, evtId)
                    ps.setLong(2, pktSeqNr)
                    ps.setObject(3, pkt.posId.toLongOrNull(), java.sql.Types.BIGINT)
                    ps.setObject(4, pkt.qty.toBigDecimalOrNull(), java.sql.Types.DECIMAL)
                    ps.setObject(5, pkt.qty3.toBigDecimalOrNull(), java.sql.Types.DECIMAL)
                    ps.setObject(6, pkt.extlBookText.toStringOrNull(), java.sql.Types.VARCHAR)
                }

                override fun getBatchSize(): Int = packets.size
            })
        } catch (e: DataAccessException) {
            log.error("Failed to upsert EVT_PKT batch: ${e.message}", e)
            throw e
        }
    }
}
