package ch.innuvation.bookingingestion.config

import ch.innuvation.bookingingestion.IntegrationTest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.test.context.TestContext
import org.springframework.test.context.TestExecutionListener
import org.testcontainers.DockerClientFactory

/**
 * TestExecutionListener implementation that ensures required test containers
 * are started concurrently before the Spring application context is initialized.
 */
class TestcontainerTestExecutionListener : TestExecutionListener {
    val containerStartFunctions =
        setOf(
            startMySqlContainer(),
        )

    override fun beforeTestClass(testContext: TestContext) {
        if (IntegrationTest::class.java.isAssignableFrom(testContext.testClass)) {
            if (DockerClientFactory.instance().isDockerAvailable) {
                runBlocking(Dispatchers.Default) {
                    containerStartFunctions.forEach { startContainer ->
                        launch { startContainer() }
                    }
                }
            }
        }
    }
}
