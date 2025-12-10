package ch.innuvation.bookingingestion

import ch.innuvation.bookingingestion.config.BookingIngestionServiceProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.transaction.annotation.EnableTransactionManagement

@SpringBootApplication
@EnableConfigurationProperties(BookingIngestionServiceProperties::class)
@EnableTransactionManagement
class BookingIngestionApplication

fun main(args: Array<String>) {
    runApplication<BookingIngestionApplication>(*args)
}
