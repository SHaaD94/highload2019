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
import kotlin.collections.ArrayList
import kotlin.concurrent.fixedRateTimer

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): List<MutableMap<String, Any?>>
    fun group(groupRequest: GroupRequest): List<Group>
}

class AccountRepositoryImpl : AccountRepository {
    private val accounts0_250 = Array<InnerAccount?>(250_000) { null }
    private val accounts250_500 = Array<InnerAccount?>(250_000) { null }
    private val accounts500_750 = Array<InnerAccount?>(250_000) { null }
    private val accounts750_1000 = Array<InnerAccount?>(250_000) { null }
    private val accounts1000_1300 = Array<InnerAccount?>(300_000) { null }
    private val accounts1300 = ConcurrentHashMap<Int, InnerAccount>(100_000)

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

    private val likeIndex0_250 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex250_500 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex500_750 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex750_1000 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex1000_1300 = Array<MutableCollection<Int>>(300_000) { ArrayList(30) }
    private val likeIndex1300 = ConcurrentHashMap<Int, MutableCollection<Int>>(100_000)

    private val premiumIndex = ConcurrentHashMap<Int, Boolean>(700_000)

    @Volatile
    private var ids = listOf<Int>()

    init {
        fixedRateTimer("", true, 5_000, 10_000) {
            try {
                ids = accounts1300.keys.sortedByDescending { it }.plus(1299999 downTo 0)
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    override fun addAccount(account: Account) {
        withLockById(account.id) {

            check(getAccountByIndex(account.id) == null) { "User ${account.id} already exists" }
            val innerAccount = InnerAccount(account.email, account.birth, account.phone, account.premium)
            when {
                account.id < 250_000 -> accounts0_250[account.id] = innerAccount
                account.id in 250_000 until 500_000 -> accounts250_500[account.id - 250_000] = innerAccount
                account.id in 500_000 until 750_000 -> accounts500_750[account.id - 500_000] = innerAccount
                account.id in 750_000 until 1_000_000 -> accounts750_1000[account.id - 750_000] = innerAccount
                account.id in 1_000_000 until 1_300_000 -> accounts1000_1300[account.id - 1_000_000] = innerAccount
                else -> accounts1300[account.id] = innerAccount
            }

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
                var collection = when {
                    likeId < 250_000 -> likeIndex0_250[likeId]
                    likeId in 250_000 until 500_000 -> likeIndex250_500[likeId - 250_000]
                    likeId in 500_000 until 750_000 -> likeIndex500_750[likeId - 500_000]
                    likeId in 750_000 until 1_000_000 -> likeIndex750_1000[likeId - 750_000]
                    likeId in 1_000_000 until 1_300_000 -> likeIndex1000_1300[likeId - 1_000_000]
                    else -> null
                }
                if (collection == null) {
                    collection = likeIndex1300.computeIfAbsent(likeId) { ArrayList(30) }
                }

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
            if (any != null) filters.add(any.flatMap { fnameIndex[it] ?: emptySet<Int>() }.toSet())
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
                likes.map { getLikesByIndex(it) as Collection<Int> }.reduce { acc, list -> acc.intersect(list) }
            }?.let { filters.add(it.toSet()) }
        }

        if (filters.any { it.isEmpty() }) {
            return emptyList()
        }

        return ids
            .asSequence()
            .filter { getAccountByIndex(it) != null }
            .filter { id -> filters.all { it == ids || it.contains(id) } }
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
                val innerAccount = getAccountByIndex(id)!!
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

    override fun group(groupRequest: GroupRequest): List<Group> {
        val filters = mutableListOf<Set<Int>>()

        groupRequest.sname?.let { snameIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.fname?.let { fnameIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.sex?.let { sexIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.birthYear?.let { queryByYear(it, birthIndex) }?.let { filters.add(it.toSet()) }
        groupRequest.joinedYear?.let { queryComplexByYear(it, joinedIndex) }?.let { filters.add(it.toSet()) }
        groupRequest.country?.let { countryIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.city?.let { cityIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.status?.let { statusIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.interests?.let { interestIndex[it] ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.likes?.let { getLikesByIndex(it).toSet() }?.let { filters.add(it) }

        //sex, status, interests, country, city

        if (filters.any { it.isEmpty() }) {
            return emptyList()
        }

//        val groups = HashMap<Group>()

        if (groupRequest.keys.contains("interests")) {

        }

        return listOf()
    }

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

    private fun withLockById(id: Int, block: () -> Unit) = synchronized(id) { block() }

    private fun getLikesByIndex(likeId: Int): MutableCollection<Int> = when {
        likeId < 250_000 -> likeIndex0_250[likeId]
        likeId in 250_000 until 500_000 -> likeIndex250_500[likeId - 250_000]
        likeId in 500_000 until 750_000 -> likeIndex500_750[likeId - 500_000]
        likeId in 750_000 until 1_000_000 -> likeIndex750_1000[likeId - 750_000]
        likeId in 1_000_000 until 1_300_000 -> likeIndex1000_1300[likeId - 1_000_000]

        else -> likeIndex1300[likeId]!!
    }

    private fun getAccountByIndex(id: Int): InnerAccount? = when {
        id < 250_000 -> accounts0_250[id]
        id in 250_000 until 500_000 -> accounts250_500[id - 250_000]
        id in 500_000 until 750_000 -> accounts500_750[id - 500_000]
        id in 750_000 until 1_000_000 -> accounts750_1000[id - 750_000]
        id in 1_000_000 until 1_300_000 -> accounts1000_1300[id - 1_000_000]
        else -> accounts1300[id]
    }
}
