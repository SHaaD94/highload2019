package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.concurrentHashSet
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
import kotlin.concurrent.fixedRateTimer

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): List<MutableMap<String, Any?>>
    fun group(groupRequest: GroupRequest): List<Group> = listOf()
}

class AccountRepositoryImpl : AccountRepository {
    init {
        fixedRateTimer("", true, 0, 10_000) {
            ids = accounts.keys().toList().sortedDescending()
        }
    }

    private val accounts = ConcurrentHashMap<Int, InnerAccount>()

    @Volatile
    private var ids = accounts.keys().toList().sortedDescending()

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

    private val joinedIndex = ConcurrentSkipListMap<Long, MutableCollection<Int>>()

    private val interestIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val likeIndex = ConcurrentHashMap<Int, MutableCollection<Int>>(1_300_000)

    private val premiumIndex = ConcurrentHashMap<Int, Boolean>()

    override fun addAccount(account: Account) {
        withLockById(account.id) {
            check(!accounts.containsKey(account.id)) { "User ${account.id} already exists" }
            accounts[account.id] = InnerAccount(account.email, account.birth, account.phone, account.premium)

            sexIndex.computeIfAbsent(account.sex) { concurrentHashSet(600_000) }.add(account.id)

            statusIndex.computeIfAbsent(account.status) { concurrentHashSet(300_000) }.add(account.id)

            account.fname?.let {
                val collection = fnameIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            } ?: fnameNullIndex.add(account.id)

            account.sname?.let {
                val collection = snameIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            } ?: snameNullIndex.add(account.id)

            account.email.let { email ->
                val domain = email.split("@").let { parsedEmail -> parsedEmail[parsedEmail.size - 1] }
                val domainCollection = emailDomainIndex.computeIfAbsent(domain) { concurrentHashSet(100_000) }
                domainCollection.add(account.id)

                emailComparingIndex.put(email, account.id)
            }
            account.phone?.let { phone ->
                val code = parsePhoneCode(phone)
                val codeCollection = phoneCodeIndex.computeIfAbsent(code) { concurrentHashSet() }
                codeCollection.add(account.id)
            } ?: phoneNullIndex.add(account.id)

            account.country?.let {
                val collection = countryIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            } ?: countryNullIndex.add(account.id)

            account.city?.let {
                val collection = cityIndex.computeIfAbsent(it) { concurrentHashSet() }
                collection.add(account.id)
            } ?: cityNullIndex.add(account.id)

            birthIndex[account.birth] = account.id
            val joinedBucket = joinedIndex.computeIfAbsent(account.joined) { ArrayList(200) }
            synchronized(joinedBucket) {
                joinedBucket.add(account.id)
            }

            (account.interests ?: emptyList()).forEach {
                val collection = interestIndex.computeIfAbsent(it) { concurrentHashSet(15_000) }
                collection.add(account.id)
            }

            (account.likes ?: emptyList()).forEach { (likeId, _) ->
                val collection = likeIndex.computeIfAbsent(likeId) { mutableListOf() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }

            account.premium?.let { (start, finish) ->
                premiumIndex.computeIfAbsent(account.id) { now() in (start until finish) }
            }
        }
    }

    override fun filter(filterRequest: FilterRequest): List<MutableMap<String, Any?>> {
        val filters = mutableListOf<Set<Int>>()
        filterRequest.email?.let { (domain, lt, gt) ->
            if (domain != null) filters.add(emailDomainIndex[domain] ?: emptySet())
            when {
                lt != null && gt != null -> emailComparingIndex.subMap(lt, gt)
                lt != null -> emailComparingIndex.headMap(lt)
                gt != null -> emailComparingIndex.tailMap(gt)
                else -> null
            }?.values?.let { filters.add(it.toSet()) }
        }

        filterRequest.fname?.let { (eq, any, _) ->
            if (eq != null) filters.add(fnameIndex[eq] ?: emptySet())
            if (any != null) filters.add(any.flatMap { fnameIndex[eq] ?: emptySet<Int>() }.toSet())
        }

        filterRequest.sname?.let { (eq, starts, _) ->
            if (starts != null) {
                filters.add(snameIndex.filter { it.key.startsWith(starts) }.flatMap { it.value }.toSet())
            }
            if (eq != null) filters.add(snameIndex[eq] ?: emptySet())
        }

        filterRequest.phone?.let { (eq, _) ->
            if (eq != null) filters.add(phoneCodeIndex[eq] ?: emptySet())
        }

        filterRequest.country?.let { (eq, _) ->
            if (eq != null) filters.add(countryIndex[eq] ?: emptySet())
        }

        filterRequest.city?.let { (eq, any, _) ->
            if (eq != null) filters.add(cityIndex[eq] ?: emptySet())
            if (any != null) filters.add(any.flatMap { cityIndex[it] ?: emptySet<Int>() }.toSet())
        }

        filterRequest.birth?.let { (lt, gt, year) ->
            when {
                lt != null && gt != null -> birthIndex.subMap(lt, gt)
                lt != null -> birthIndex.headMap(lt)
                gt != null -> birthIndex.tailMap(gt)
                else -> null
            }?.values?.let { filters.add(it.toSet()) }

            if (year != null) filters.add(queryByYear(year, birthIndex).toSet())
        }

        filterRequest.interests?.let { (contains, any) ->
            contains?.let { interests ->
                interests.map { interestIndex[it] ?: emptyList<Int>() }.reduce { l, r -> l.intersect(r) }
            }?.let { filters.add(it.toSet()) }
            any?.let { interests ->
                interests.flatMap { interestIndex[it] ?: emptyList<Int>() }
            }?.let { filters.add(it.toSet()) }
        }

        filterRequest.likes?.let { (contains) ->
            contains?.let { likes ->
                likes.map { likeIndex[it] ?: emptyList<Int>() }.reduce { acc, list -> acc.intersect(list) }
            }?.let { filters.add(it.toSet()) }
        }

        return ids
            .asSequence()
            .filter { id ->
                filters.all { it == ids || it.contains(id) }
            }
            .filter { id ->
                filterByNull(filterRequest.fname?.nill, fnameNullIndex, id) &&
                        filterByNull(filterRequest.sname?.nill, snameNullIndex, id) &&
                        filterByNull(filterRequest.phone?.nill, phoneNullIndex, id) &&
                        filterByNull(filterRequest.country?.nill, countryNullIndex, id) &&
                        filterByNull(filterRequest.city?.nill, cityNullIndex, id)
            }
            .filter { id ->
                filterRequest.premium?.let { (now, nill) ->
                    val filterByNow = if (now != null) premiumIndex[id] == true else true
                    val filterByNill = if (nill != null) {
                        when (nill) {
                            true -> premiumIndex.containsKey(id)
                            false -> !premiumIndex.containsKey(id)
                        }
                    } else true
                    filterByNill && filterByNow
                } ?: true
            }
            .filter { id -> if (filterRequest.sex != null) sexIndex[filterRequest.sex.eq]!!.contains(id) else true }
            .filter { id ->
                filterRequest.status?.let { (eq, neq) ->
                    val eqDecision = eq?.let { statusIndex[it]!!.contains(id) } ?: true
                    val neqDecision = neq?.let { !statusIndex[it]!!.contains(id) } ?: true
                    neqDecision && eqDecision
                } ?: true
            }
            .take(filterRequest.limit)
            .map { id ->
                val innerAccount = accounts[id]!!
                val resultObj = mutableMapOf<String, Any?>("id" to id, "email" to innerAccount.email)

                filterRequest.sex?.let { resultObj["sex"] = filterRequest.sex.eq }
                filterRequest.status?.let {
                    resultObj["status"] = statusIndex.entries.first { it.value.contains(id) }.key
                }
                filterRequest.fname?.let {
                    resultObj["fname"] = fnameIndex.entries.firstOrNull { it.value.contains(id) }?.key
                }
                filterRequest.sname?.let {
                    resultObj["sname"] = snameIndex.entries.firstOrNull { it.value.contains(id) }?.key
                }
                filterRequest.phone?.let { resultObj["phone"] = innerAccount.phone }
                filterRequest.country?.let {
                    resultObj["country"] = countryIndex.entries.firstOrNull { it.value.contains(id) }?.key
                }
                filterRequest.city?.let {
                    resultObj["city"] = cityIndex.entries.firstOrNull { it.value.contains(id) }?.key
                }
                filterRequest.birth?.let { resultObj["birth"] = innerAccount.birth }
                filterRequest.premium?.let { resultObj["premium"] = innerAccount.premium }

                resultObj
            }
            .toList()
    }

    private val emptyInterestsList = listOf(null)
//    override fun group(groupRequest: GroupRequest): List<Group> {
//        val filteredIds = customIntersects(ids,
//            groupRequest.sname?.let { snameIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.fname?.let { fnameIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.sex?.let { sexIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.birthYear?.let { queryByYear(it, birthIndex) } ?: ids,
//            groupRequest.joinedYear?.let { queryComplexByYear(it, joinedIndex) } ?: ids,
//            groupRequest.country?.let { countryIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.city?.let { cityIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.status?.let { statusIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.interests?.let { interestIndex[it] ?: emptySet<Int>() } ?: ids,
//            groupRequest.likes?.let { likeIndex[it] ?: emptySet<Int>() } ?: ids
//        )
//
//        val groups = HashMap<String, Int>(filteredIds.size)
//        filteredIds
//            .asSequence()
//            .map { accounts[it]!! }
//            .forEach { acc ->
//                val keyWoInterests = StringJoiner("|")
//
//                if (groupRequest.keys.contains("sex")) keyWoInterests.add(acc.sex.toString())
//                if (groupRequest.keys.contains("status")) keyWoInterests.add(acc.status)
//                if (groupRequest.keys.contains("country")) keyWoInterests.add(acc.country)
//                if (groupRequest.keys.contains("city")) keyWoInterests.add(acc.city)
//
//                val resultKeyWoInterests = keyWoInterests.toString()
//                if (groupRequest.keys.contains("interests")) {
//                    (acc.interests ?: emptyInterestsList).forEach {
//                        val resultKey =
//                            if (resultKeyWoInterests.isEmpty()) it.toString() else "$resultKeyWoInterests|$it"
//                        groups[resultKey] = (groups[resultKey] ?: 0) + 1
//                    }
//                } else {
//                    groups[resultKeyWoInterests] = (groups[resultKeyWoInterests] ?: 0) + 1
//                }
//            }
//        return groups.entries.let {
//            when {
//                groupRequest.order > 0 -> it.sortedWith(compareBy({ it.value }, { it.key }))
//                else -> it.sortedWith(compareByDescending<MutableMap.MutableEntry<String, Int>> { it.value }.thenByDescending { it.key })
//            }
//        }
//            .take(groupRequest.limit)
//            .map { (key, count) ->
//                val splitKey = key.split("|")
//                var keyCounter = 0
//
//                var sexChecked = false
//                var statusChecked = false
//                var countryChecked = false
//                var cityChecked = false
//                var interestsChecked = false
//
//                var sex: Char? = null
//                var status: String? = null
//                var country: String? = null
//                var city: String? = null
//                var interests: String? = null
//                while (keyCounter < splitKey.size) {
//                    when {
//                        groupRequest.keys.contains("sex") && !sexChecked -> {
//                            sex = nullToString(splitKey[keyCounter])?.get(0)
//                            sexChecked = true
//                        }
//                        groupRequest.keys.contains("status") && !statusChecked -> {
//                            status = nullToString(splitKey[keyCounter])
//                            statusChecked = true
//                        }
//                        groupRequest.keys.contains("country") && !countryChecked -> {
//                            country = nullToString(splitKey[keyCounter])
//                            countryChecked = true
//                        }
//                        groupRequest.keys.contains("city") && !cityChecked -> {
//                            city = nullToString(splitKey[keyCounter])
//                            cityChecked = true
//                        }
//                        groupRequest.keys.contains("interests") && !interestsChecked -> {
//                            interests = nullToString(splitKey[keyCounter])
//                            interestsChecked = true
//                        }
//                    }
//                    keyCounter++
//                }
//                Group(count, sex, status, interests, country, city)
//            }
//    }

    private fun nullToString(s: String) = if (s == "null") null else s

    private fun queryByYear(year: Int, index: NavigableMap<Long, Int>): Collection<Int> {
        val from = LocalDateTime.of(year, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC)
        val to = LocalDateTime.of(year + 1, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) - 1
        return index.subMap(from, to).values
    }

    private fun queryComplexByYear(year: Int, index: NavigableMap<Long, MutableCollection<Int>>): Collection<Int> {
        val from = LocalDateTime.of(year, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC)
        val to = LocalDateTime.of(year + 1, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) - 1
        return index.subMap(from, to).flatMap { it.value }
    }

    private fun filterByNull(nill: Boolean?, index: Set<Int>, id: Int) =
        if (nill != null) {
            when (nill) {
                true -> !index.contains(id)
                false -> index.contains(id)
            }
        } else true

    private fun withLockById(id: Int, block: () -> Unit) = synchronized(id.toString().intern()) { block() }
}