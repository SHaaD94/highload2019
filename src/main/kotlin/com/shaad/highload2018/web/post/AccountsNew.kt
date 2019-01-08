package com.shaad.highload2018.web.post

import com.google.inject.Inject
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsNew @Inject constructor(private val accountsRepository: AccountRepository) : HandlerBase() {
    private val path = "/accounts/new/".toByteArray()

    private val okResponse = HandlerAnswer(201, emptyResponse)

    override fun method(): HttpVerb = HttpVerb.POST

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
        return try {
            accountsRepository.addAccount(objectMapper.readValue<Account>(buf[bodyRange], Account::class.java))
            okResponse
        } catch (e: Exception) {
            HandlerAnswer(400, emptyResponse)
        }
    }
}
