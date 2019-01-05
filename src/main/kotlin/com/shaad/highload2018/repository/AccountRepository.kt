package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.utils.concurrentHashSet
import com.shaad.highload2018.utils.customIntersects
import com.shaad.highload2018.utils.now
import com.shaad.highload2018.utils.parsePhoneCode
import com.shaad.highload2018.web.get.FilterRequest
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.collections.HashMap

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): List<Account>
    fun group(groupRequest: GroupRequest): List<Group>
}
//todo prefix tree for sname

class AccountRepositoryImpl : AccountRepository {
    private val accounts = ConcurrentHashMap<Int, Account>(10000)

    private val statusIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val sexIndex = ConcurrentHashMap<Char, MutableSet<Int>>()

    private val fnameIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val fnameNullIndex = concurrentHashSet<Int>()

    private val snameIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val snameNullIndex = concurrentHashSet<Int>()

    private val emailDomainIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val emailComparingIndex = ConcurrentSkipListMap<String, Int>()

    private val phoneCodeIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val phoneNullIndex = concurrentHashSet<Int>()

    private val countryIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val countryNullIndex = concurrentHashSet<Int>()

    private val cityIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val cityNullIndex = concurrentHashSet<Int>()

    private val birthIndex = ConcurrentSkipListMap<Long, Int>()

    private val joinedIndex = ConcurrentSkipListMap<Long, Int>()

    private val interestIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val likeIndex = ConcurrentHashMap<Int, MutableSet<Int>>()

    private val premiumIndex = ConcurrentHashMap<Int, Boolean>()

    override fun addAccount(account: Account) {
        withLockById(account.id) {
            check(accounts[account.id] == null) { "User ${account.id} already exists" }
            accounts[account.id] = account

            sexIndex.computeIfAbsent(account.sex) { concurrentHashSet() }.add(account.id)

            statusIndex.computeIfAbsent(account.status) { concurrentHashSet() }.add(account.id)

            account.fname?.let {
                fnameNullIndex.add(account.id)
                val collection = fnameIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            }
            account.sname?.let {
                snameNullIndex.add(account.id)
                val collection = snameIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            }
            account.email.let { email ->
                val domain = email.split("@").let { parsedEmail -> parsedEmail[parsedEmail.size - 1] }
                val domainCollection = emailDomainIndex.computeIfAbsent(domain) { concurrentHashSet() }
                domainCollection.add(account.id)

                emailComparingIndex.put(email, account.id)
            }
            account.phone?.let { phone ->
                phoneNullIndex.add(account.id)
                val code = parsePhoneCode(phone)
                val codeCollection = phoneCodeIndex.computeIfAbsent(code) { concurrentHashSet() }
                codeCollection.add(account.id)

            }
            account.country?.let {
                countryNullIndex.add(account.id)
                val collection = countryIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            }
            account.city?.let {
                cityNullIndex.add(account.id)
                val collection = cityIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            }
            account.birth.let { birthIndex.put(it, account.id) }
            account.joined.let { joinedIndex.put(it, account.id) }

            (account.interests ?: emptyList()).forEach {
                val collection = interestIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            }
            (account.likes ?: emptyList()).forEach { (likeId, _) ->
                val collection = likeIndex.computeIfAbsent(likeId) { concurrentHashSet() }
                collection.add(account.id)
            }

            account.premium?.let { (start, finish) ->
                premiumIndex.computeIfAbsent(account.id) { now() in (start until finish) }
            }
        }
    }

    override fun filter(filterRequest: FilterRequest): List<Account> {
        val ids: Collection<Int> = accounts.keys().toList()

        val filteredByEmail = filterRequest.email?.let { (domain, lt, gt) ->
            val filteredByDomain = if (domain != null) emailDomainIndex[domain] ?: emptyList() else ids
            val filteredByBorders = when {
                lt != null && gt != null -> emailComparingIndex.subMap(lt, gt)
                lt != null -> emailComparingIndex.headMap(lt)
                gt != null -> emailComparingIndex.tailMap(gt)
                else -> null
            }?.values ?: ids
            customIntersects(ids, filteredByDomain, filteredByBorders)
        } ?: ids

        val filteredByFname = filterRequest.fname?.let { (eq, any, nill) ->
            val filteredByEq = if (eq != null) fnameIndex[eq] ?: emptyList() else ids
            val filteredByAny = if (any != null) any.flatMap { fnameIndex[eq] ?: emptyList<Int>() } else ids

            val result = customIntersects(ids, filteredByEq, filteredByAny)
            filterByNull(nill, fnameNullIndex, result)
        } ?: ids

        val filteredBySname = filterRequest.sname?.let { (eq, _, nill) ->
            val filteredByEq = if (eq != null) snameIndex[eq] ?: emptyList() else ids
            filterByNull(nill, snameNullIndex, filteredByEq)
        } ?: ids

        val filteredByPhone = filterRequest.phone?.let { (eq, nill) ->
            val filteredByEq = if (eq != null) phoneCodeIndex[eq] ?: emptyList() else ids
            filterByNull(nill, phoneNullIndex, filteredByEq)
        } ?: ids

        val filteredByCountry = filterRequest.country?.let { (eq, nill) ->
            val filteredByEq = if (eq != null) countryIndex[eq] ?: emptyList() else ids
            filterByNull(nill, countryNullIndex, filteredByEq)
        } ?: ids

        val filteredByCity = filterRequest.city?.let { (eq, any, nill) ->
            val filteredByEq = if (eq != null) cityIndex[eq] ?: emptyList() else ids
            val filteredByAny = if (any != null) any.flatMap { cityIndex[it] ?: emptyList<Int>() } else ids

            val result = customIntersects(ids, filteredByEq, filteredByAny)
            filterByNull(nill, cityNullIndex, result)
        } ?: ids

        val filteredByBirth = filterRequest.birth?.let { (lt, gt, year) ->
            val filteredByBorders = when {
                lt != null && gt != null -> birthIndex.subMap(lt, gt)
                lt != null -> birthIndex.headMap(lt)
                gt != null -> birthIndex.tailMap(gt)
                else -> null
            }?.values ?: ids

            val filteredByYear = if (year != null) queryByYear(year, birthIndex) else ids
            customIntersects(ids, filteredByBorders, filteredByYear)
        } ?: ids

        val filteredByInterests = filterRequest.interests?.let { (contains, any) ->
            val filteredByContains = contains?.let { interests ->
                interests.map { interestIndex[it] ?: emptyList<Int>() }.reduce { l, r -> l.intersect(r) }
            } ?: ids
            val filteredByAny = any?.let { interests ->
                interests.flatMap { interestIndex[it] ?: emptyList<Int>() }.toSet()
            } ?: ids
            customIntersects(ids, filteredByContains, filteredByAny)
        } ?: ids

        val filteredByLikes = filterRequest.likes?.let { (contains) ->
            contains?.let { likes ->
                likes.map { likeIndex[it] ?: emptyList<Int>() }.reduce { l, r -> l.intersect(r) }
            } ?: ids
        } ?: ids

        val firstResult = customIntersects(
            ids,
            filteredByEmail,
            filteredByFname,
            filteredBySname,
            filteredByPhone,
            filteredByCountry,
            filteredByCity,
            filteredByBirth,
            filteredByInterests,
            filteredByLikes
        )

        val filteredByPremium = filterRequest.premium?.let { (now, nill) ->
            val filteredByNow = if (now != null) firstResult.filter { premiumIndex[it] == true } else firstResult
            if (nill != null) {
                when (nill) {
                    true -> filteredByNow.filter { premiumIndex.containsKey(it) }
                    false -> filteredByNow.filter { !premiumIndex.containsKey(it) }
                }
            } else filteredByNow
        } ?: firstResult

        return filteredByPremium
            .sortedDescending()
            .asSequence()
            .filter { id -> if (filterRequest.sex != null) sexIndex[filterRequest.sex.eq]!!.contains(id) else true }
            .filter { id ->
                filterRequest.status?.let { (eq, neq) ->
                    val eqDecision = eq?.let { statusIndex[it]!!.contains(id) } ?: true
                    val neqDecision = neq?.let { !statusIndex[it]!!.contains(id) } ?: true
                    neqDecision && eqDecision
                } ?: true
            }
            .map { accounts[it]!! }
            .filter { if (filterRequest.sname?.starts != null) it.sname != null && it.sname.startsWith(filterRequest.sname.starts) else true }
            .take(filterRequest.limit)
            .toList()
    }

    private val emptyInterestsList = listOf(null)
    override fun group(groupRequest: GroupRequest): List<Group> {
        val ids = accounts.keys().toList()
        val filteredIds = customIntersects(ids,
            groupRequest.sname?.let { snameIndex[it] ?: emptySet<Int>() } ?: ids,
            groupRequest.fname?.let { fnameIndex[it] ?: emptySet<Int>() } ?: ids,
            //groupRequest.phone?.let {  }
            groupRequest.sex?.let { sexIndex[it] ?: emptySet<Int>() } ?: ids,
            groupRequest.birthYear?.let { queryByYear(it, birthIndex) } ?: ids,
            groupRequest.joinedYear?.let { queryByYear(it, joinedIndex) } ?: ids,
            groupRequest.country?.let { countryIndex[it] ?: emptySet<Int>() } ?: ids,
            groupRequest.city?.let { cityIndex[it] ?: emptySet<Int>() } ?: ids,
            groupRequest.status?.let { statusIndex[it] ?: emptySet<Int>() } ?: ids,
            groupRequest.interests?.let { interestIndex[it] ?: emptySet<Int>() } ?: ids,
            groupRequest.likes?.let { likeIndex[it] ?: emptySet<Int>() } ?: ids
        )

        val groups = HashMap<String, Int>(filteredIds.size)
        filteredIds
            .asSequence()
            .map { accounts[it]!! }
            .forEach { acc ->
                val keyWoInterests = StringJoiner("|")

                when {
                    groupRequest.keys.contains("sex") -> keyWoInterests.add(acc.sex.toString())
                    groupRequest.keys.contains("status") -> keyWoInterests.add(acc.status)
                    groupRequest.keys.contains("country") -> keyWoInterests.add(acc.country)
                    groupRequest.keys.contains("city") -> keyWoInterests.add(acc.city)
                }
                val resultKeyWoInterests = keyWoInterests.toString()
                if (groupRequest.keys.contains("interests")) {
                    (acc.interests ?: emptyInterestsList).forEach {
                        val resultKey =
                            if (resultKeyWoInterests.isEmpty()) it.toString() else "$resultKeyWoInterests|$it"
                        groups[resultKey] = (groups[resultKey] ?: 0) + 1
                    }
                } else {
                    groups[resultKeyWoInterests] = (groups[resultKeyWoInterests] ?: 0) + 1
                }
            }
        return groups.entries.let {
            when {
                groupRequest.order > 0 -> it.sortedBy { it.component2() }
                else -> it.sortedByDescending { it.component2() }
            }
        }
            .take(groupRequest.limit)
            .map { (key, count) ->
                val splitKey = key.split("|")
                var keyCounter = 0

                var sexChecked = false
                var statusChecked = false
                var countryChecked = false
                var cityChecked = false
                var interestsChecked = false

                var sex: Char? = null
                var status: String? = null
                var country: String? = null
                var city: String? = null
                var interests: String? = null
                while (keyCounter < splitKey.size) {
                    when {
                        groupRequest.keys.contains("sex") && !sexChecked -> {
                            sex = nullToString(splitKey[keyCounter])?.get(0)
                            sexChecked = true
                        }
                        groupRequest.keys.contains("status") && !statusChecked -> {
                            status = nullToString(splitKey[keyCounter])
                            statusChecked = true
                        }
                        groupRequest.keys.contains("country") && !countryChecked -> {
                            country = nullToString(splitKey[keyCounter])
                            countryChecked = true
                        }
                        groupRequest.keys.contains("city") && !cityChecked -> {
                            city = nullToString(splitKey[keyCounter])
                            cityChecked = true
                        }
                        groupRequest.keys.contains("interests") && !interestsChecked -> {
                            interests = nullToString(splitKey[keyCounter])
                            interestsChecked = true
                        }
                    }
                    keyCounter++
                }
                Group(count, sex, status, interests, country, city)
            }
    }

    fun nullToString(s: String) = if (s == "null") null else s

    private fun queryByYear(year: Int, index: NavigableMap<Long, Int>): Collection<Int> {
        val from = LocalDateTime.of(year, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC)
        val to = LocalDateTime.of(year + 1, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) - 1
        return index.tailMap(from).headMap(to).values
    }

    private fun filterByNull(nill: Boolean?, index: Set<Int>, collectionToFilter: Collection<Int>) =
        if (nill != null) {
            when (nill) {
                true -> collectionToFilter.filter { index.contains(it) }
                false -> collectionToFilter.filter { !index.contains(it) }
            }
        } else collectionToFilter

    private fun withLockById(id: Int, block: () -> Unit) = synchronized(id.toString().intern()) { block() }
}