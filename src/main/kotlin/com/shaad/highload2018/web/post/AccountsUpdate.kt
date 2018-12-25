package com.shaad.highload2018.web.post

import com.shaad.highload2018.web.Handler
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsUpdate : Handler {
    private val regex = "/accounts/\\d+/".toRegex()
    override fun method(): HttpVerb = HttpVerb.POST

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        regex.matches(buf.get(pathRange))

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): ByteArray {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
