package ch.innuvation.bookingingestion.proto

import com.avaloq.acp.bde.protobuf.Wrappers
import java.math.BigDecimal
import java.time.LocalDate
import java.time.format.DateTimeFormatter

fun Wrappers.SInt64Value?.toLongOrNull(): Long? = this?.value

fun Wrappers.StringValue?.toBigDecimalOrNull(): BigDecimal? =
    this?.value?.let { BigDecimal(it) }

fun Wrappers.StringValue?.toLocalDateOrNull(): LocalDate? =
    this?.value?.let { LocalDate.parse(it) }

fun Wrappers.StringValue?.toStringOrNull(): String? =
    this?.value

fun String.toLongOrNull(): Long? = this.toLongOrNull()

fun String.toLocalDateOrNull(pattern: String = "yyyy-MM-dd"): LocalDate? =
    runCatching { LocalDate.parse(this, DateTimeFormatter.ofPattern(pattern)) }.getOrNull()
