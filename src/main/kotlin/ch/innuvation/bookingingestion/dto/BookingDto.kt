package ch.innuvation.bookingingestion.dto

import java.math.BigDecimal
import java.time.LocalDate

data class BookingDto(
    val evtId: Long,
    val buId: Long?,
    val evtStatusId: Long?,
    val veriDate: LocalDate?,
    val bookDate: LocalDate?,
    val valDate: LocalDate?,
    val trxDate: LocalDate?,
    val perfDate: LocalDate?,
    val posId: Long?,
    val qty: BigDecimal?,
    val qty3: BigDecimal?,
    val extlBookText: String?
)
