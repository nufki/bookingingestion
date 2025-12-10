package ch.innuvation.bookingingestion.service

import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.repository.BooksRepository
import ch.innuvation.bookingingestion.repository.EvtPktRepository
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.Books
import com.avaloq.acp.bde.protobuf.books.EvtPkt
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class BookingIngestionService(
    private val booksRepository: BooksRepository,
    private val evtPktRepository: EvtPktRepository,
) {

    private val log = logger()

    /**
     * Ingest a batch of Books messages (no tombstones).
     */
    @Transactional
    suspend fun ingestBatch(booksMessages: List<Books>) =
        withContext(Dispatchers.IO) {
            if (booksMessages.isEmpty()) return@withContext

            // 1) upsert all BOOKS rows in one shot
            booksRepository.upsertBatch(booksMessages)

            // 2) collect all EVT_PKT rows (evtId + pkt)
            val allPackets: List<Pair<Long, EvtPkt>> =
                booksMessages.flatMap { msg ->
                    val evtId = msg.evtId.toLongOrNull()
                        ?: throw IllegalArgumentException("evtId is required")

                    msg.evtPktList.map { pkt -> evtId to pkt }
                }

            // 3) upsert all EVT_PKT rows in one shot
            evtPktRepository.upsertBatch(allPackets)

            if (log.isDebugEnabled) {
                log.debug(
                    "Ingested ${booksMessages.size} Books and ${allPackets.size} EVT_PKT rows"
                )
            }
        }
}
