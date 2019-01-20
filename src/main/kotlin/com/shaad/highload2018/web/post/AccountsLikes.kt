package com.shaad.highload2018.web.post

import com.shaad.highload2018.repository.addLike
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import com.wizzardo.tools.json.JsonTools
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsLikes : HandlerBase() {
    private val path = "/accounts/likes/".toByteArray()
    override fun method(): HttpVerb = HttpVerb.POST

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer =
        try {
            val body = JsonTools.parse(buf[bodyRange]).asJsonObject()
            body["likes"]!!.asJsonArray().forEach { l ->
                val like = l.asJsonObject()
                val likee = like.getAsInteger("likee")!!
                val ts = like.getAsInteger("ts")
                val liker = like.getAsInteger("liker")

                addLike(likee, ts, liker)
            }


            HandlerAnswer(202, emptyResponse)
        } catch (e: Exception) {
            HandlerAnswer(400, emptyResponse)
        }
}
