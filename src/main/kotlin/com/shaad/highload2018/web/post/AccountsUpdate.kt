package com.shaad.highload2018.web.post

import com.shaad.highload2018.repository.updateUser
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import com.wizzardo.tools.json.JsonTools
import org.rapidoid.buffer.Buf
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsUpdate : HandlerBase() {
    private val regex = "/accounts/\\d+/".toRegex()
    override fun method(): HttpVerb = HttpVerb.POST

    private val requestPathStart = "/accounts/".toByteArray()
    private val requestPathEnd = "/".toByteArray()


    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        regex.matches(buf.get(pathRange))

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
        return try {
            val id = buf[BufRange(
                pathRange.start + requestPathStart.size,
                pathRange.start + pathRange.length - requestPathStart.size - requestPathEnd.size - pathRange.start
            )].toInt()

            val body = JsonTools.parse(buf[bodyRange]).asJsonObject()
            check(!body.containsKey("id")) { "should not contain id" }
            val newEmail = body.getAsString("email")
            val newFname = body.getAsString("fname")
            val newSname = body.getAsString("sname")
            val newPhone = body.getAsString("phone")
            val newSex = body.getAsString("sex")
            val newBirth = body.getAsInteger("birth")
            val newCity = body.getAsString("city")
            val newCountry = body.getAsString("country")
            val newJoined = body.getAsInteger("joined")
            val newStatus = body.getAsString("status")
            val newInterests = body.get("interests")?.asJsonArray()
            val newPremium = body.get("premium")?.asJsonObject()
            val newLikes = body.get("likes")?.asJsonArray()

            updateUser(
                id,
                newEmail,
                newFname,
                newSname,
                newPhone,
                newSex,
                newBirth,
                newJoined,
                newCity,
                newCountry,
                newStatus,
                newInterests,
                newPremium,
                newLikes
            )

            return HandlerAnswer(202, emptyResponse)
        } catch (e: Exception) {
            HandlerAnswer(400, emptyResponse)
        }
    }
}
