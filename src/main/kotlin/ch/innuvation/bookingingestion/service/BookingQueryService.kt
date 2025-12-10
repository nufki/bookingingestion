package ch.innuvation.bookingingestion.service

import ch.innuvation.bookingingestion.dto.BookingDto
import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import kotlinx.coroutines.reactive.awaitSingle
import org.jooq.DSLContext
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux

@Service
class BookingQueryService(
    private val dsl: DSLContext
) {

    // Now reactive + coroutine friendly
    suspend fun getBookingsByPosId(posId: Long): List<BookingDto> {
        val query = dsl.select(
            BOOKS.EVT_ID,
            BOOKS.BU_ID,
            BOOKS.EVT_STATUS_ID,
            BOOKS.VERI_DATE,
            BOOKS.BOOK_DATE,
            BOOKS.VAL_DATE,
            BOOKS.TRX_DATE,
            BOOKS.PERF_DATE,
            EVT_PKT.POS_ID,
            EVT_PKT.BOOK_KIND_ID,
            EVT_PKT.QTY,
            EVT_PKT.EXTL_BOOK_TEXT
        )
            .from(BOOKS)
            .join(EVT_PKT)
            .on(BOOKS.EVT_ID.eq(EVT_PKT.EVT_ID))
            .where(EVT_PKT.POS_ID.eq(posId))

        return Flux.from(query) // jOOQ ResultQuery is a Publisher<Record>
            .map { r ->
                BookingDto(
                    evtId = r[BOOKS.EVT_ID]!!,
                    buId = r[BOOKS.BU_ID],
                    evtStatusId = r[BOOKS.EVT_STATUS_ID],
                    veriDate = r[BOOKS.VERI_DATE],
                    bookDate = r[BOOKS.BOOK_DATE],
                    valDate = r[BOOKS.VAL_DATE],
                    trxDate = r[BOOKS.TRX_DATE],
                    perfDate = r[BOOKS.PERF_DATE],
                    posId = r[EVT_PKT.POS_ID],
                    bookKindId = r[EVT_PKT.BOOK_KIND_ID],
                    qty = r[EVT_PKT.QTY],
                    extlBookText = r[EVT_PKT.EXTL_BOOK_TEXT]
                )
            }
            .collectList()
            .awaitSingle()   // suspend here, returns List<BookingDto>
    }
}
