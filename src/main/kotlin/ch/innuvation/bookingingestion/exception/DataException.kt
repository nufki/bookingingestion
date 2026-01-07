package ch.innuvation.bookingingestion.exception

import org.springframework.dao.DataAccessResourceFailureException
import org.slf4j.Logger

/**
 * Handles database operation failures by logging and throwing appropriate Spring exceptions.
 */
object DatabaseExceptionHandler {

    fun handleBulkUpsertFailure(operation: String, e: Exception, log: Logger): Nothing {
        log.error("Failed to bulk upsert $operation: ${e.message}", e)
        throw DataAccessResourceFailureException("Bulk upsert $operation failed", e)
    }

    fun handleBulkDeleteFailure(operation: String, e: Exception, log: Logger): Nothing {
        log.error("Failed to bulk delete $operation: ${e.message}", e)
        throw DataAccessResourceFailureException("Bulk delete $operation failed", e)
    }

    fun handleBulkOperationFailure(operation: String, e: Exception, log: Logger): Nothing {
        log.error("Failed to execute bulk operation for $operation: ${e.message}", e)
        throw DataAccessResourceFailureException("Bulk operation $operation failed", e)
    }
}