package com.shaad.highload2018.web.get

import com.google.inject.Inject
import com.shaad.highload2018.repository.*
import com.shaad.highload2018.utils.ByteArrayBuilder
import com.shaad.highload2018.web.HandlerAnswer
import com.shaad.highload2018.web.HandlerBase
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsFilter @Inject constructor(private val repository: AccountRepository) : HandlerBase() {
    private val path = "/accounts/filter/".toByteArray()

    private val accountsStart = "{\"accounts\":[".toByteArray()
    private val accountsEnd = "]}".toByteArray()

    private val idBytes = "\"id\":".toByteArray()
    private val emailBytes = "\"email\":\"".toByteArray()
    private val sexBytes = "\"sex\":\"".toByteArray()
    private val fnameBytes = "\"fname\":\"".toByteArray()
    private val snameBytes = "\"sname\":\"".toByteArray()
    private val statusBytes = "\"status\":\"".toByteArray()
    private val phoneBytes = "\"phone\":\"".toByteArray()
    private val countryBytes = "\"country\":\"".toByteArray()
    private val cityBytes = "\"city\":\"".toByteArray()
    private val birthBytes = "\"birth\":".toByteArray()
    private val premiumStart = "\"premium\":{\"start\":".toByteArray()
    private val premiumFinish = ",\"finish\":".toByteArray()

    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): HandlerAnswer {
        val params = parseParams(buf, paramsRange)

        val filterRequest = try {
            parseFilterRequest(params)
        } catch (e: Exception) {
            return HandlerAnswer(400, e.message!!.toByteArray())
        }

        var firstAccount = true
        val bytes = ByteArrayBuilder()
        bytes.append(accountsStart)
        repository.filter(filterRequest)
            .forEach { acc ->
                if (firstAccount) firstAccount = false else bytes.append(comma)
                bytes
                    .append(figuredBracketOpen)
                    //id
                    .append(idBytes)
                    .append(acc.id!!.toString().toByteArray())
                    .append(comma)
                    //email
                    .append(emailBytes)
                    .append(acc.email)
                    .append(quotes)

                if (filterRequest.sex != null) {
                    bytes.append(comma)
                    bytes.append(sexBytes)
                    bytes.append(int2Sex(acc.sex)!!.toString().toByteArray())
                    bytes.append(quotes)
                }
                if (filterRequest.status != null) {
                    bytes.append(comma)
                    bytes.append(statusBytes)
                    bytes.append(statusesInv[acc.status]!!.toByteArray())
                    bytes.append(quotes)
                }
                if (filterRequest.fname != null) {
                    val fname = acc.fname?.let { fnamesInv[it] }
                    if (fname != null) {
                        bytes.append(comma)
                        bytes.append(fnameBytes)
                        bytes.append(fname.toByteArray())
                        bytes.append(quotes)
                    }
                }
                if (filterRequest.sname != null) {
                    val sname = acc.sname?.let { snamesInv[it] }
                    if (sname != null) {
                        bytes.append(comma)
                        bytes.append(snameBytes)
                        bytes.append(sname.toByteArray())
                        bytes.append(quotes)
                    }
                }
                if (filterRequest.phone != null) {
                    if (acc.phone != null) {
                        bytes.append(comma)
                        bytes.append(phoneBytes)
                        bytes.append(acc.phone)
                        bytes.append(quotes)
                    }
                }
                if (filterRequest.country != null) {
                    val country = acc.country?.let { countriesInv[it] }
                    if (country != null) {
                        bytes.append(comma)
                        bytes.append(countryBytes)
                        bytes.append(country.toByteArray())
                        bytes.append(quotes)
                    }
                }
                if (filterRequest.city != null) {
                    val city = acc.city?.let { citiesInv[it] }
                    if (city != null) {
                        bytes.append(comma)
                        bytes.append(cityBytes)
                        bytes.append(city.toByteArray())
                        bytes.append(quotes)
                    }
                }
                if (filterRequest.birth != null) {
                    bytes.append(comma)
                    bytes.append(birthBytes)
                    bytes.append(acc.birth.toString().toByteArray())
                }
                if (filterRequest.premium != null) {
                    if (acc.premiumStart != null) {
                        bytes.append(comma)
                        bytes.append(premiumStart)
                        bytes.append(acc.premiumStart.toString().toByteArray())
                        bytes.append(premiumFinish)
                        bytes.append(acc.premiumFinish.toString().toByteArray())
                        bytes.append(figuredBracketClose)
                    }
                }
                bytes.append(figuredBracketClose)
            }
        bytes.append(accountsEnd)
        val result = bytes.toArray()

        return HandlerAnswer(200, result)
    }

    private fun parseFilterRequest(params: Map<String, String>): FilterRequest {
        var limit = -1
        var sexEq: Char? = null
        var emailDomain: String? = null
        var emailGt: String? = null
        var emailLt: String? = null

        var statusEq: String? = null
        var statusNeq: String? = null

        var snameEq: String? = null
        var snameStarts: String? = null
        var snameNull: String? = null

        var fnameEq: String? = null
        var fnameAny: String? = null
        var fnameNull: String? = null

        var phoneCode: String? = null
        var phoneNull: String? = null

        var countryEq: String? = null
        var countryNull: String? = null

        var cityEq: String? = null
        var cityAny: String? = null
        var cityNull: String? = null

        var birthLt: Int? = null
        var birthGt: Int? = null
        var birthYear: Int? = null

        var interestsContains: String? = null
        var interestsAny: String? = null

        var likesContains: String? = null

        var premiumNow: String? = null
        var premiumNull: String? = null

        params.forEach { (param, value) ->
            when (param) {
                "limit" -> limit = value.toInt()
                "sex_eq" -> sexEq = value[0]

                "email_domain" -> emailDomain = value
                "email_lt" -> emailLt = value
                "email_gt" -> emailGt = value

                "status_eq" -> statusEq = value
                "status_neq" -> statusNeq = value

                "sname_eq" -> snameEq = value
                "sname_starts" -> snameStarts = value
                "sname_null" -> snameNull = value

                "fname_eq" -> fnameEq = value
                "fname_any" -> fnameAny = value
                "fname_null" -> fnameNull = value

                "phone_code" -> phoneCode = value
                "phone_null" -> phoneNull = value

                "country_eq" -> countryEq = value
                "country_null" -> countryNull = value

                "city_eq" -> cityEq = value
                "city_any" -> cityAny = value
                "city_null" -> cityNull = value

                "birth_lt" -> birthLt = value.toInt()
                "birth_gt" -> birthGt = value.toInt()
                "birth_year" -> birthYear = value.toInt()

                "interests_contains" -> interestsContains = value
                "interests_any" -> interestsAny = value

                "likes_contains" -> likesContains = value

                "premium_now" -> premiumNow = value
                "premium_null" -> premiumNull = value

                "query_id" -> {
                }

                else -> throw RuntimeException("Unsupported param $param")
            }
        }

        require(limit >= 0) { "limit should be specified" }

        val sexRequest = sexEq?.let { SexRequest(it) }

        val emailRequest = if (emailDomain != null || emailGt != null || emailLt != null)
            EmailRequest(emailDomain, emailLt?.toByteArray(), emailGt?.toByteArray())
        else null

        val statusRequest = if (statusEq != null || statusNeq != null)
            StatusRequest(statusEq, statusNeq)
        else null

        val fnameRequest = if (fnameAny != null || fnameEq != null || fnameNull != null)
            FnameRequest(fnameEq, fnameAny?.split(","), parseNull(fnameNull))
        else null

        val snameRequest = if (snameEq != null || snameStarts != null || snameNull != null)
            SnameRequest(snameEq, snameStarts, parseNull(snameNull))
        else null

        val phoneRequest = if (phoneCode != null || phoneNull != null)
            PhoneRequest(phoneCode, parseNull(phoneNull))
        else null

        val countryRequest = if (countryEq != null || countryNull != null)
            CountryRequest(countryEq, parseNull(countryNull))
        else null

        val cityRequest = if (cityAny != null || cityEq != null || cityNull != null)
            CityRequest(cityEq, cityAny?.split(","), parseNull(cityNull))
        else null

        val birthRequest = if (birthGt != null || birthLt != null || birthYear != null)
            BirthRequest(birthLt, birthGt, birthYear)
        else null

        val interestsRequest = if (interestsAny != null || interestsContains != null)
            InterestsRequest(interestsContains?.split(","), interestsAny?.split(","))
        else null

        val likesRequest = if (likesContains != null)
            LikesRequest(likesContains?.split(",")?.map { it.toInt() })
        else null

        val premiumRequest = if (premiumNow != null || premiumNull != null)
            PremiumRequest(parseNull(premiumNow), parseNull(premiumNull))
        else null

        val filterRequest = FilterRequest(
            limit,
            sexRequest, emailRequest, statusRequest, fnameRequest, snameRequest, phoneRequest,
            countryRequest, cityRequest, birthRequest, interestsRequest, likesRequest, premiumRequest
        )
        return filterRequest
    }

    private fun parseNull(value: String?): Boolean? =
        when (value) {
            "0" -> true
            "1" -> false
            null -> null
            else -> throw RuntimeException("Incorrect null $value")
        }
}

data class SexRequest(val eq: Char)
data class EmailRequest(
    val domain: String?,
    val lt: ByteArray?,
    val gt: ByteArray?
)

data class StatusRequest(
    val eq: String?,
    val neq: String?
)

data class FnameRequest(
    val eq: String?,
    val any: List<String>?,
    val nill: Boolean?
)

data class SnameRequest(
    val eq: String?,
    val starts: String?,
    val nill: Boolean?
)

data class PhoneRequest(
    val code: String?,
    val nill: Boolean?
)

data class CountryRequest(
    val eq: String?,
    val nill: Boolean?
)

data class CityRequest(
    val eq: String?,
    val any: List<String>?,
    val nill: Boolean?
)

data class BirthRequest(
    val lt: Int?,
    val gt: Int?,
    val year: Int?
)

data class InterestsRequest(
    val contains: List<String>?,
    val any: List<String>?
)

data class LikesRequest(
    val contains: List<Int>?
)

data class PremiumRequest(
    val now: Boolean?,
    val nill: Boolean?
)

class FilterRequest(
    val limit: Int,
    val sex: SexRequest?,
    val email: EmailRequest?,
    val status: StatusRequest?,
    val fname: FnameRequest?,
    val sname: SnameRequest?,
    val phone: PhoneRequest?,
    val country: CountryRequest?,
    val city: CityRequest?,
    val birth: BirthRequest?,
    val interests: InterestsRequest?,
    val likes: LikesRequest?,
    val premium: PremiumRequest?
)
