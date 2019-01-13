package com.shaad.highload2018.web

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb
import org.rapidoid.util.Msc

interface Handler {
    fun method(): HttpVerb
    fun matches(buf: Buf, pathRange: BufRange): Boolean
    fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer
}

abstract class HandlerBase : Handler {
    protected val emptyResponse = "{}".toByteArray()
    protected val comma = ",".toByteArray()
    protected val quadBracketOpen = "[".toByteArray()
    protected val quadBracketClose = "]".toByteArray()
    protected val figuredBracketOpen = "{".toByteArray()
    protected val figuredBracketClose = "}".toByteArray()
    protected val colon = ":".toByteArray()
    protected val quotes = "\"".toByteArray()
    protected val commaQuotes = "\",".toByteArray()

    protected val objectMapper = jacksonObjectMapper()

    init {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    }

    protected fun parseParams(buf: Buf, paramsRange: BufRange): Map<String, String> {
        return buf[paramsRange]
            .split("&")
            .map { it.split("=") }
            .map { it[0] to Msc.urlDecodeOrKeepOriginal(it[1]) }
            .toMap()
    }
}

class HandlerAnswer(val code: Int, val body: ByteArray)