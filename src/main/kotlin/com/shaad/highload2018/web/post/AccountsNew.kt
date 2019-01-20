package com.shaad.highload2018.web.post

import com.shaad.highload2018.repository.addUser
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import com.wizzardo.tools.json.JsonTools
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsNew : HandlerBase() {
    private val path = "/accounts/new/".toByteArray()

    private val okResponse = HandlerAnswer(201, emptyResponse)

    override fun method(): HttpVerb = HttpVerb.POST

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
        return try {
            val body = JsonTools.parse(buf[bodyRange]).asJsonObject()
            val id = body.getAsInteger("id")!!
            val newEmail = body.getAsString("email")!!
            val newFname = body.getAsString("fname")!!
            val newSname = body.getAsString("sname")!!
            val newPhone = body.getAsString("phone")!!
            val newSex = body.getAsString("sex")!!
            val newBirth = body.getAsInteger("birth")!!
            val newCity = body.getAsString("city")!!
            val newCountry = body.getAsString("country")!!
            val newJoined = body.getAsInteger("joined")!!
            val newStatus = body.getAsString("status")!!
            val newInterests = body["interests"]!!.asJsonArray()
            val newPremium = body["premium"]!!.asJsonObject()
            val newLikes = body["likes"]!!.asJsonArray()

            addUser(
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
            return okResponse
        } catch (e: Exception) {
            HandlerAnswer(400, emptyResponse)
        }
    }
}
