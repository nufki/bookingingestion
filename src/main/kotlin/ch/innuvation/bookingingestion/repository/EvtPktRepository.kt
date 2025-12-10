package ch.innuvation.bookingingestion.repository

import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import ch.innuvation.bookingingestion.proto.toBigDecimalOrNull
import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.proto.toStringOrNull
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.EvtPkt
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.springframework.stereotype.Repository
import reactor.kotlin.core.publisher.toMono

@Repository
class EvtPktRepository(
    private val jooq: DSLContext,
) {

    private val log = logger()

    /**
     * Batch insert/upsert all EVT_PKT rows for a batch of events.
     * Param is a list of (evtId, pkt) pairs.
     */
    suspend fun upsertBatch(packets: List<Pair<Long, EvtPkt>>) {
        if (packets.isEmpty()) return

        val insert = jooq.insertInto(
            EVT_PKT,
            EVT_PKT.EVT_ID,
            EVT_PKT.POS_ID,
            EVT_PKT.BOOK_KIND_ID,
            EVT_PKT.QTY,
            EVT_PKT.EXTL_BOOK_TEXT
        ).apply {
            packets.forEach { (evtId, pkt) ->
                values(
                    evtId,
                    pkt.posId.toLongOrNull(),
                    pkt.bookKindId.toLongOrNull(),
                    pkt.qty.toBigDecimalOrNull(),
                    pkt.extlBookText.toStringOrNull()
                )
            }
        }
            .onDuplicateKeyUpdate()
            .setAllToExcluded()
            .toMono()

        insert
            .doOnError { e -> log.error("Failed to upsert EVT_PKT batch: ${getError(e)}", e) }
            .awaitSingle()
    }

    private fun getError(t: Throwable): String? {
        return if (t is DataAccessException) {
            val cause = t.getCause(R2dbcException::class.java)
            cause?.message ?: t.message
        } else t.message
    }
}
