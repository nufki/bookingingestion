package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import ch.innuvation.bookingingestion.proto.toBigDecimalOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.proto.toStringOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.EvtPkt
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.springframework.stereotype.Repository
import java.math.BigInteger

@Repository
class EvtPktRepository(
    private val jooq: DSLContext,
) {

    private val log = logger()

    /**
     * Batch insert/upsert all EVT_PKT rows for a batch of events.
     * Param is a list of (evtId, pkt) pairs.
     */
    fun upsertBatch(packets: List<Pair<Long, EvtPkt>>) {
        if (packets.isEmpty()) return

        val insert = jooq.insertInto(
            EVT_PKT,
            EVT_PKT.EVT_ID,
            EVT_PKT.PKT_SEQ_NR,
            EVT_PKT.POS_ID,
            EVT_PKT.BOOK_KIND_ID,
            EVT_PKT.QTY,
            EVT_PKT.EXTL_BOOK_TEXT
        ).apply {
            packets.forEach { (evtId, pkt) ->
                val pktSeqNr = pkt.pktSeqNr.toLongOrNull()
                    ?: throw IllegalArgumentException("pktSeqNr is required")
                values(
                    BigInteger.valueOf(evtId),
                    BigInteger.valueOf(pktSeqNr),
                    pkt.posId.toLongOrNull()?.let { BigInteger.valueOf(it) },
                    pkt.bookKindId.toLongOrNull()?.let { BigInteger.valueOf(it) },
                    pkt.qty.toBigDecimalOrNull(),
                    pkt.extlBookText.toStringOrNull()
                )
            }
        }
            .onDuplicateKeyUpdate()
            .setAllToExcluded()

        try {
            insert.execute()
        } catch (e: DataAccessException) {
            log.error("Failed to upsert EVT_PKT batch: ${e.message}", e)
            throw e
        }
    }
}
