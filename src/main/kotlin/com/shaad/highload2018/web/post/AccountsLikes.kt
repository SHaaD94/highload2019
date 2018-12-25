package com.shaad.highload2018.web.post

import com.shaad.highload2018.web.Handler
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsLikes : Handler {
    private val path = "/accounts/likes/".toByteArray()
    override fun method(): HttpVerb = HttpVerb.POST

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): ByteArray {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
