package ch.innuvation.bookingingestion.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory.getLogger


/**
 * helper to simplify logger creation
 */
inline fun <reified T> T.logger(): Logger = getLogger(T::class.java)
