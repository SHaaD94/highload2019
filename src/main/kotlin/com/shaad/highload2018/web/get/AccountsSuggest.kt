package com.shaad.highload2018.web.get

import com.shaad.highload2018.web.Handler
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsSuggest : Handler {
    private val regex = "/accounts/\\d+/suggest/".toRegex()
    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        regex.matches(buf.get(pathRange))

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): ByteArray {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
