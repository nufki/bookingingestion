package ch.innuvation.bookingingestion.controller

import ch.innuvation.bookingingestion.dto.BookingDto
import ch.innuvation.bookingingestion.service.BookingQueryService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class BookingController(
    private val bookingQueryService: BookingQueryService
) {

    @GetMapping("/bookings/position/{posId}")
    fun getBookingsByPosition(@PathVariable posId: Long): List<BookingDto> =
        bookingQueryService.getBookingsByPosId(posId)
}
