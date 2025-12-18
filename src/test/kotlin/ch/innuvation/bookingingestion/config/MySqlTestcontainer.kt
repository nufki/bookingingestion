package ch.innuvation.bookingingestion.config

import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy
import org.testcontainers.utility.DockerImageName

/**
 * Extension to start mysql in integration-tests.
 */
private val mySqlContainer: MySQLContainer<*> =
    MySQLContainer(DockerImageName.parse("mysql:8"))
        .withDatabaseName("BOOKING_INGESTION_DB")
        .withCommand("mysqld", "--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci")
        .withInitScripts()
        .withEnv(
            mapOf(
                "MYSQL_ROOT_PASSWORD" to "books",
            ),
        ).waitingFor(HostPortWaitStrategy())

fun startMySqlContainer(): () -> Unit =
    {
        if (!mySqlContainer.isRunning) mySqlContainer.start()

        System.setProperty(
            "spring.datasource.url",
            "jdbc:mysql://${mySqlContainer.host}:${
                mySqlContainer.getMappedPort(
                    MySQLContainer.MYSQL_PORT,
                )
            }/${mySqlContainer.databaseName}",
        )
        System.setProperty("spring.datasource.username", mySqlContainer.username)
        System.setProperty("spring.datasource.password", mySqlContainer.password)
    }
