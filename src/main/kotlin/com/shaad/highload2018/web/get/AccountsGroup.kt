package com.shaad.highload2018.web.get

import com.shaad.highload2018.repository.*
import com.shaad.highload2018.utils.ByteArrayBuilder
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsGroup : HandlerBase() {
    private val path = "/accounts/group/".toByteArray()

    private val groupsStart = "{\"groups\":[".toByteArray()
    private val groupsEnd = "]}".toByteArray()

    private val countBytes = "\"count\":".toByteArray()
    private val sexBytes = "\"sex\":\"".toByteArray()
    private val statusBytes = "\"status\":\"".toByteArray()
    private val interesBytes = "\"interests\":\"".toByteArray()
    private val countryBytes = "\"country\":\"".toByteArray()
    private val cityBytes = "\"city\":\"".toByteArray()


    private val allowedGroupKeys = hashSetOf("sex", "status", "interests", "country", "city")

    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
        val params = parseParams(buf, paramsRange)

        val request = try {
            GroupRequest(
                params["sname"],
                params["fname"],
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
                params["keys"]!!.split(",").toList().let {
                    if (it.any { !allowedGroupKeys.contains(it) }) throw RuntimeException("Wrong keys $it")
                    else it
                }
            )
        } catch (e: Exception) {
            return HandlerAnswer(400, e.message?.toByteArray() ?: "wrong input".toByteArray())
        }

        val bytes = ByteArrayBuilder()
        bytes.append(groupsStart)
        var first = true
        group(request).forEach { group ->
            if (first) first = false else bytes.append(comma)
            bytes
                .append(figuredBracketOpen)
                .append(countBytes)
                .append(group.count)
            if (group.sex != null) {
                bytes.append(comma)
                bytes.append(sexBytes)
                bytes.append(int2Sex(group.sex))
                bytes.append(quotes)
            }
            if (group.status != null) {
                bytes.append(comma)
                bytes.append(statusBytes)
                bytes.append(group.status.let { statusesInv[it] }!!.toByteArray())
                bytes.append(quotes)
            }
            if (group.country != null) {
                bytes.append(comma)
                bytes.append(countryBytes)
                bytes.append(group.country.let { countriesInv[it] }!!.toByteArray())
                bytes.append(quotes)
            }
            if (group.city != null) {
                bytes.append(comma)
                bytes.append(cityBytes)
                bytes.append(group.city.let { citiesInv[it] }!!.toByteArray())
                bytes.append(quotes)
            }
            if (group.interest != null) {
                bytes.append(comma)
                bytes.append(interesBytes)
                bytes.append(group.interest.let { interestsInv[it] }!!.toByteArray())
                bytes.append(quotes)
            }
            bytes.append(figuredBracketClose)
        }
        bytes.append(groupsEnd)

        return HandlerAnswer(
            200,
            bytes.toArray()
        )
    }
}


data class GroupRequest(
    val sname: String?,
    val fname: String?,
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
    val keys: List<String>
)

data class Group(
    val sex: Int?,
    val status: Int?,
    val interest: Int?,
    val country: Int?,
    val city: Int?,
    val count: Int
)