package ch.innuvation.bookingingestion

import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.springframework.core.io.ClassPathResource
import java.io.File
import java.io.InputStreamReader
import java.io.Reader

fun <T : Message.Builder> getProtobufFromJson(fileName: String, message: T): T {
    JsonFormat.parser().merge(getFile(fileName), message)
    return message
}

fun <T : Message.Builder> getProtobufFromJson(file: File, message: T): T {
    JsonFormat.parser().merge(file.reader(), message)
    return message
}

fun getAvroFromJson(file: File, schema: Schema): GenericData.Record =
    AvroSchemaUtils.toObject(
        file.readText(),
        AvroSchema(schema),
    ) as GenericData.Record

fun getFile(fileName: String): Reader = InputStreamReader(ClassPathResource(fileName).inputStream, Charsets.UTF_8)
