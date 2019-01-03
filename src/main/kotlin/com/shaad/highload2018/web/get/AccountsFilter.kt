package com.shaad.highload2018.web.get

import com.google.inject.Inject
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.web.Handler
import org.rapidoid.buffer.Buf
import org.rapidoid.bytes.BytesUtil
import org.rapidoid.data.BufRange
import org.rapidoid.http.HttpVerb

class AccountsFilter @Inject constructor(val repository: AccountRepository) : Handler {
    private val path = "/accounts/filter/".toByteArray()
    override fun method(): HttpVerb = HttpVerb.GET

    override fun matches(buf: Buf, pathRange: BufRange): Boolean =
        BytesUtil.match(buf.bytes(), pathRange.start, path, true)

    override fun process(buf: Buf, pathRange: BufRange, paramsRange: BufRange, bodyRange: BufRange): ByteArray {

        val filterRequest = FilterRequest(
            null, null, null, null, null, null,
            null, null, null, null, null, null
        )
        repository.filter(filterRequest)

        return "".toByteArray()
    }
}

class Eq<T>(val value: T)
class Neq<T>(val value: T)
class Lt<T>(val value: T)
class Gt<T>(val value: T)
class Null(val exists: Boolean)

class SexRequest(val eq: Eq<Char>)
class EmailRequest(
    val domain: String,
    val lt: Lt<Long>?,
    val gt: Gt<Long>?
)

class StatusRequest(
    val eq: Eq<String>?,
    val neq: Neq<String>?
)

class FnameRequest(
    val eq: Eq<String>?,
    val any: List<Eq<String>>?,
    val nill: List<Null>?
)

class SnameRequest(
    val eq: Eq<String>?,
    val starts: String?,
    val nill: List<Null>?
)

class PhoneRequest(
    val code: String?,
    val nill: Null?
)

class CountryRequest(
    val eq: Eq<String>?,
    val nill: Null?
)

class CityRequest(
    val eq: Eq<String>?,
    val any: List<Eq<String>>?,
    val nill: List<Null>?
)

class BirthRequest(
    val lt: Lt<Long>?,
    val gt: Gt<Long>?,
    val year: Int?
)

class InterestsRequest(
    val contains: List<String>?,
    val any: List<String>?
)

class LikesRequest(
    val any: List<Int>
)

class PremiumRequest(
    val now: Boolean?,
    val nill: Null?
)

class FilterRequest(
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
