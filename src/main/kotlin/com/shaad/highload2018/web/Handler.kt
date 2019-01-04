package com.shaad.highload2018.web

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb
import org.rapidoid.util.Msc

interface Handler {
    fun method(): HttpVerb
    fun matches(buf: Buf, pathRange: BufRange): Boolean
    fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): ByteArray
}

abstract class HandlerBase : Handler {
    protected val objectMapper = jacksonObjectMapper()

    protected fun parseParams(buf: Buf, paramsRange: BufRange): Map<String, String> {
        return buf[paramsRange]
            .split("&")
            .map { it.split("=") }
            .filter { it.size == 2 }
            .map { it[0] to Msc.urlDecodeOrKeepOriginal(it[1]) }
            .toMap()
    }
}
