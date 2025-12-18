import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

fun main() {
    val mapper = jacksonObjectMapper()

    // 1. Load from classpath
    val resource = Thread.currentThread().contextClassLoader
        .getResource("books_export/books.json")
        ?: error("books_export/books.json not found on classpath")

    val root = mapper.readTree(resource)

    val items = root["results"][0]["items"]

    // 2. Output dir (relative to project root, adjust as needed)
    val outDir = Path.of("src/test/resources/books")
    Files.createDirectories(outDir)

    for (item in items) {
        val evtJsonString = item["evt_json"].asText()
        val evtNode = mapper.readTree(evtJsonString)
        val evtId = evtNode["evtId"]["value"].asText()

        val prettyJson = mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsBytes(evtNode)

        val outFile = outDir.resolve("$evtId.json")
        Files.write(
            outFile,
            prettyJson,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        )
    }
}
