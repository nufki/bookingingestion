package ch.innuvation.bookingingestion.config

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.oracle.OracleContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration

/**
 * Extension to start Oracle in integration-tests.
 * Uses oracle-free image which supports ARM64 (Apple Silicon).
 */
private val oracleContainer: OracleContainer =
    OracleContainer(DockerImageName.parse("gvenzl/oracle-free:23-slim-faststart"))
        //.withDatabaseName("FREEPDB1") // its always this name in oracle free!
        .withUsername("books")
        .withPassword("books")
        .waitingFor(
            Wait.forLogMessage(".*DATABASE IS READY TO USE!.*", 1)
                .withStartupTimeout(Duration.ofMinutes(5))
        )

fun startOracleContainer(): () -> Unit = {
    if (!oracleContainer.isRunning) oracleContainer.start()

    val url = oracleContainer.jdbcUrl
    val user = oracleContainer.username
    val pass = oracleContainer.password

    System.setProperty("spring.datasource.url", url)
    System.setProperty("spring.datasource.username", user)
    System.setProperty("spring.datasource.password", pass)

    System.setProperty("spring.flyway.url", url)
    System.setProperty("spring.flyway.user", user)
    System.setProperty("spring.flyway.password", pass)
}


