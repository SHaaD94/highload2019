package com.shaad.highload2018.web.get

import com.google.inject.Inject
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsRecommend @Inject constructor(private val accountRepository: AccountRepository) : HandlerBase() {
    private val regex = "/accounts/\\d+/recommend/".toRegex()
    private val requestPathStart = "/accounts/".toByteArray()
    private val requestPathEnd = "/recommend/?".toByteArray()
    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        regex.matches(buf.get(pathRange))

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
//        val id = buf[BufRange(
//            pathRange.start + requestPathStart.size,
//            pathRange.length - requestPathStart.size - requestPathEnd.size
//        )].toInt()
//
//        parseParams(buf, paramsRange)

        return HandlerAnswer(200, emptyResponse)
    }
}
