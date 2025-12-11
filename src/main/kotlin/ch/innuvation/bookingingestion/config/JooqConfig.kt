package ch.innuvation.bookingingestion.config

import ch.innuvation.bookingingestion.utils.logger
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.ExecuteContext
import org.jooq.ExecuteListener
import org.jooq.impl.DSL
import org.jooq.impl.DefaultConfiguration
import org.jooq.impl.DefaultExecuteListenerProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.slf4j.Logger
import javax.sql.DataSource

@Configuration
class JooqConfig {

    private val log = logger()

    @Bean
    fun dslContext(dataSource: DataSource): DSLContext {
        val configuration = DefaultConfiguration().apply {
            setDataSource(dataSource)
            setSQLDialect(SQLDialect.MYSQL)
            set(DefaultExecuteListenerProvider(LoggingExecuteListener(log)))
        }
        return DSL.using(configuration)
    }

    /**
     * Lightweight SQL logger similar to previous R2DBC proxy logging.
     */
    private class LoggingExecuteListener(private val log: Logger) : ExecuteListener {
        override fun executeStart(ctx: ExecuteContext) {
            if (log.isDebugEnabled) {
                val sql = ctx.sql()
                val bindings = ctx.query()
                    ?.bindValues?.joinToString(prefix = "[", postfix = "]") { value -> value?.toString() ?: "null" }
                    ?: "[]"
                log.debug("executing queries:\n$sql")
                log.debug("with bindings:\n$bindings")
            }
        }
    }
}
