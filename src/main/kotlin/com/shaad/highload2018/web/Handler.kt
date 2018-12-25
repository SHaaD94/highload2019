package com.shaad.highload2018.web

import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

interface Handler {
    fun method(): HttpVerb
    fun matches(buf: Buf, pathRange: BufRange): Boolean
    fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): ByteArray
}
