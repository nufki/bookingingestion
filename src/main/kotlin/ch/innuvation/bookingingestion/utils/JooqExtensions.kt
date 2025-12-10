package ch.innuvation.bookingingestion.utils

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.jooq.Field
import org.jooq.JSON
import org.jooq.impl.DSL
import java.util.*

class JooqExtensions {
    companion object {

        val mapper = ObjectMapper()

        fun <T> jsonExtract(field: Field<*>, jsonPath: String, type: Class<T>): Field<T?> {
            return DSL.field("json_unquote(json_extract({0}, {1}))", field, DSL.inline(jsonPath))
                .cast(type)
        }


        fun jsonExtractI18nString(jsonField: JSON?): Map<String, String?> {

            if (jsonField == null) {
                return emptyMap()
            }

            val stringMap: Map<String, String> = mapper.readValue(jsonField.data(), object : TypeReference<Map<String, String>>() {})

            return mapOf(
                Locale.GERMAN.toLanguageTag() to stringMap[Locale.GERMAN.toLanguageTag()],
                Locale.ITALIAN.toLanguageTag() to stringMap[Locale.ITALIAN.toLanguageTag()],
                Locale.FRENCH.toLanguageTag() to stringMap[Locale.FRENCH.toLanguageTag()],
                Locale.ENGLISH.toLanguageTag() to stringMap[Locale.ENGLISH.toLanguageTag()],
                "automatic" to stringMap["automatic"]
            )
        }
    }
}
