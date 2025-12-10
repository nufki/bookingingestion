package ch.innuvation.bookingingestion.config

import ch.innuvation.bookingingestion.utils.logger
import io.r2dbc.proxy.ProxyConnectionFactory
import io.r2dbc.proxy.core.Bindings
import io.r2dbc.spi.ConnectionFactory
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class JooqConfig {

    private val log = logger()

    @Bean
    fun jooqConfiguration(connectionFactory: ConnectionFactory): DSLContext {
        val proxiedConnection = ProxyConnectionFactory.builder(connectionFactory)
            .onBeforeQuery {
                if(log.isDebugEnabled) {
                    log.debug("executing queries: \n${
                        it.queries.joinToString(separator = "\n") { queryInfo -> queryInfo.query }
                    }")
                    log.debug("with bindings: \n${
                        it.queries.flatMap { queries -> queries.bindingsList }.joinToString(separator = "\n") { query ->
                            getUsedBindings(query)
                                .joinToString { binding -> "${binding.key}: ${binding.boundValue.value}" }
                        }
                    }"
                    )
                }
            }
            .build()
        return DSL.using(proxiedConnection)
    }

    private fun getUsedBindings(it: Bindings) = if (it.indexBindings.size > 0) {
        it.indexBindings
    } else {
        it.namedBindings
    }
}
