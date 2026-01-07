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
                buId = rs.getObject("BU_ID")?.let { (it as Number).toLong() },
                evtStatusId = rs.getObject("EVT_STATUS_ID")?.let { (it as Number).toLong() },
                veriDate = rs.getDate("VERI_DATE")?.toLocalDate(),
                bookDate = rs.getDate("BOOK_DATE")?.toLocalDate(),
                valDate = rs.getDate("VAL_DATE")?.toLocalDate(),
                trxDate = rs.getDate("TRX_DATE")?.toLocalDate(),
                perfDate = rs.getDate("PERF_DATE")?.toLocalDate(),
                posId = rs.getObject("POS_ID")?.let { (it as Number).toLong() },
                qty = rs.getBigDecimal("QTY"),
                qty3 = rs.getBigDecimal("QTY3"),
                extlBookText = rs.getString("EXTL_BOOK_TEXT")
            )
        }, posId)
    }
}
