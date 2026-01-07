package ch.innuvation.bookingingestion.service

import ch.innuvation.bookingingestion.dto.BookingDto
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.sql.Date
import javax.sql.DataSource

@Service
class BookingQueryService(
    dataSource: DataSource
) {
    private val jdbcTemplate = JdbcTemplate(dataSource)

    fun getBookingsByPosId(posId: Long): List<BookingDto> {
        val sql = """
            SELECT 
                b.EVT_ID,
                b.BU_ID,
                b.EVT_STATUS_ID,
                b.VERI_DATE,
                b.BOOK_DATE,
                b.VAL_DATE,
                b.TRX_DATE,
                b.PERF_DATE,
                e.POS_ID,
                e.QTY,
                e.QTY3,
                e.EXTL_BOOK_TEXT
            FROM BOOKS b
            INNER JOIN EVT_PKT e ON b.EVT_ID = e.EVT_ID
            WHERE e.POS_ID = ?
        """.trimIndent()

        return jdbcTemplate.query(sql, { rs, _ ->
            BookingDto(
                evtId = rs.getLong("EVT_ID"),
                buId = rs.getObject("BU_ID", Long::class.java),
                evtStatusId = rs.getObject("EVT_STATUS_ID", Long::class.java),
                veriDate = rs.getObject("VERI_DATE", Date::class.java)?.toLocalDate(),
                bookDate = rs.getObject("BOOK_DATE", Date::class.java)?.toLocalDate(),
                valDate = rs.getObject("VAL_DATE", Date::class.java)?.toLocalDate(),
                trxDate = rs.getObject("TRX_DATE", Date::class.java)?.toLocalDate(),
                perfDate = rs.getObject("PERF_DATE", Date::class.java)?.toLocalDate(),
                posId = rs.getObject("POS_ID", Long::class.java),
                qty = rs.getObject("QTY", BigDecimal::class.java),
                qty3 = rs.getObject("QTY3", BigDecimal::class.java),
                extlBookText = rs.getString("EXTL_BOOK_TEXT")
            )
        }, posId)
    }
}
