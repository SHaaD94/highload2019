package com.shaad.highload2018.web.get

import com.google.inject.Inject
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.repository.fnamesInv
import com.shaad.highload2018.repository.snamesInv
import com.shaad.highload2018.repository.statusesInv
import com.shaad.highload2018.utils.ByteArrayBuilder
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsRecommend @Inject constructor(private val accountRepository: AccountRepository) : HandlerBase() {
    private val regex = "/accounts/\\d+/recommend/".toRegex()
    private val requestPathStart = "/accounts/".toByteArray()
    private val requestPathEnd = "/recommend/".toByteArray()

    private val accountsStart = "{\"accounts\":[".toByteArray()
    private val accountsEnd = "]}".toByteArray()

    private val idBytes = "\"id\":".toByteArray()
    private val emailBytes = "\"email\":\"".toByteArray()
    private val fnameBytes = "\"fname\":\"".toByteArray()
    private val snameBytes = "\"sname\":\"".toByteArray()
    private val statusBytes = "\"status\":\"".toByteArray()
    private val birthBytes = "\"birth\":".toByteArray()
    private val premiumStart = "\"premium\":{\"start\":".toByteArray()
    private val premiumFinish = ",\"finish\":".toByteArray()

    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        regex.matches(buf.get(pathRange))

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {

        val id = buf[BufRange(
            pathRange.start + requestPathStart.size,
            pathRange.start + pathRange.length - requestPathStart.size - requestPathEnd.size - pathRange.start
        )].toInt()

        val params = parseParams(buf, paramsRange)

        val city = params["city"]
        val country = params["country"]
        val limit = params["limit"]!!.toInt()

        val bytes = ByteArrayBuilder()
        var firstAccount = true
        bytes.append(accountsStart)
        accountRepository.recommend(id, city, country, limit)
            .forEach { acc ->
                if (firstAccount) firstAccount = false else bytes.append(comma)
                bytes
                    .append(figuredBracketOpen)
                    //id
                    .append(idBytes)
                    .append(acc.id)
                    .append(comma)
                    //email
                    .append(emailBytes)
                    .append(acc.email)
                    .append(quotes)
                    .append(comma)
                    //status
                    .append(statusBytes)
                    .append(statusesInv[acc.status]!!.toByteArray())
                    .append(quotes)
                    .append(comma)
                    //birth
                    .append(birthBytes)
                    .append(acc.birth)
//                id, email, status, fname, sname, birth, premium
                if (acc.fname != null) {
                    val fname = fnamesInv[acc.fname]
                    if (fname != null) {
                        bytes.append(comma)
                        bytes.append(fnameBytes)
                        bytes.append(fname.toByteArray())
                        bytes.append(quotes)
                    }
                }
                if (acc.sname != null) {
                    val sname = snamesInv[acc.sname]
                    if (sname != null) {
                        bytes.append(comma)
                        bytes.append(snameBytes)
                        bytes.append(sname.toByteArray())
                        bytes.append(quotes)
                    }
                }
                if (acc.premium != null) {
                    bytes.append(comma)
                    bytes.append(premiumStart)
                    bytes.append(acc.premium.start)
                    bytes.append(premiumFinish)
                    bytes.append(acc.premium.finish)
                    bytes.append(figuredBracketClose)
                }
                bytes.append(figuredBracketClose)
            }
        bytes.append(accountsEnd)
        val result = bytes.toArray()

        return HandlerAnswer(200, result)
    }
}
