package ch.innuvation.bookingingestion.service

import ch.innuvation.bookingingestion.dto.BookingDto
import ch.innuvation.bookingingestion.jooq.tables.references.BOOKS
import ch.innuvation.bookingingestion.jooq.tables.references.EVT_PKT
import org.jooq.DSLContext
import org.springframework.stereotype.Service
import java.math.BigInteger

@Service
class BookingQueryService(
    private val dsl: DSLContext
) {

    fun getBookingsByPosId(posId: Long): List<BookingDto> =
        dsl.select(
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
            .where(EVT_PKT.POS_ID.eq(BigInteger.valueOf(posId)))
            .fetch { r ->
                BookingDto(
                    evtId = (r[BOOKS.EVT_ID])?.toLong() ?: throw IllegalStateException("evtId is required"),
                    buId = (r[BOOKS.BU_ID])?.toLong(),
                    evtStatusId = (r[BOOKS.EVT_STATUS_ID])?.toLong(),
                    veriDate = r[BOOKS.VERI_DATE],
                    bookDate = r[BOOKS.BOOK_DATE],
                    valDate = r[BOOKS.VAL_DATE],
                    trxDate = r[BOOKS.TRX_DATE],
                    perfDate = r[BOOKS.PERF_DATE],
                    posId = (r[EVT_PKT.POS_ID])?.toLong(),
                    bookKindId = (r[EVT_PKT.BOOK_KIND_ID])?.toLong(),
                    qty = r[EVT_PKT.QTY],
                    extlBookText = r[EVT_PKT.EXTL_BOOK_TEXT]
                )
            }
}
