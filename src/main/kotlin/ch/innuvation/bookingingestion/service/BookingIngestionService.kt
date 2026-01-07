package ch.innuvation.bookingingestion.service

import ch.innuvation.bookingingestion.proto.toLongOrNull
import ch.innuvation.bookingingestion.repository.BooksRepository
import ch.innuvation.bookingingestion.repository.EvtPktRepository
import ch.innuvation.bookingingestion.utils.logger
import com.avaloq.acp.bde.protobuf.books.Books
import com.avaloq.acp.bde.protobuf.books.EvtPkt
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class BookingIngestionService(
    private val booksRepository: BooksRepository,
    private val evtPktRepository: EvtPktRepository,
) {
    private val log = logger()

    @Transactional
    fun ingestBatch(booksMessages: List<Books>, tombstoneKeys: List<String>) {
        // 1) Handle deletions first (tombstones)
        if (tombstoneKeys.isNotEmpty()) {
            val evtIdsToDelete = tombstoneKeys.mapNotNull { it.toLongOrNull() }
            booksRepository.deleteBatch(evtIdsToDelete)
            log.info("Deleted ${evtIdsToDelete.size} BOOKS records (and cascaded EVT_PKT)")
        }

        // 2) Handle upserts
        if (booksMessages.isEmpty()) return

        booksRepository.upsertBatch(booksMessages)

        val allPackets: List<Pair<Long, EvtPkt>> =
            booksMessages.flatMap { msg ->
                val evtId = msg.evtId.toLongOrNull()
                    ?: throw IllegalArgumentException("evtId is required")
                msg.evtPktList.map { evtId to it }
            }

        evtPktRepository.upsertBatch(allPackets)

        log.info("Upserted ${booksMessages.size} BOOKS and ${allPackets.size} EVT_PKT rows")
    }
}