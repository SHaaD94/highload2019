package com.shaad.highload2018.web.get

import com.google.inject.Inject
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsGroup @Inject constructor(private val accountRepository: AccountRepository) : HandlerBase() {
    private val path = "/accounts/group/".toByteArray()
    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
        val params = parseParams(buf, paramsRange)

        val request = try {
            GroupRequest(
                params["sname"],
                params["fname"],
                /*params["phone"]*/null,
                params["sex"]?.get(0),
                params["birth"]?.toInt(),
                params["country"],
                params["city"],
                params["joined"]?.toInt(),
                params["status"],
                params["interests"],
                params["likes"]?.toInt(),
                params["limit"]!!.toInt(),
                params["order"]?.toInt() ?: 1,
                params["keys"]!!.split(",").toHashSet()
            )
        } catch (e: Exception) {
            return HandlerAnswer(400, e.message!!.toByteArray())
        }

        return HandlerAnswer(
            200,
            objectMapper.writeValueAsBytes(mapOf("groups" to accountRepository.group(request)))
        )
    }
}


data class Group(
    val count: Int,
    val sex: Char?,
    val status: String?,
    val interests: String?,
    val country: String?,
    val city: String?
)

data class GroupRequest(
    val sname: String?,
    val fname: String?,
    val phone: String?,
    val sex: Char?,
    val birthYear: Int?,
    val country: String?,
    val city: String?,
    val joinedYear: Int?,
    val status: String?,
    val interests: String?,
    val likes: Int?,
    val limit: Int,
    val order: Int,
    val keys: Set<String>
)