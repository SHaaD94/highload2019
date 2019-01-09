package com.shaad.highload2018.repository

import com.infinitydb.map.air.AirConcurrentMap
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.FilterRequest
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayList
import kotlin.concurrent.fixedRateTimer

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): List<MutableMap<String, Any?>>
    fun group(groupRequest: GroupRequest): List<Group>
}

class AccountRepositoryImpl : AccountRepository {
    // indexes
    private val accounts0_250 = Array<InnerAccount?>(250_000) { null }
    private val accounts250_500 = Array<InnerAccount?>(250_000) { null }
    private val accounts500_750 = Array<InnerAccount?>(250_000) { null }
    private val accounts750_1000 = Array<InnerAccount?>(250_000) { null }
    private val accounts1000_1300 = Array<InnerAccount?>(300_000) { null }
    private val accounts1300 = ConcurrentHashMap<Int, InnerAccount>(100_000)

    private val statusIndex = ConcurrentHashMap<Int, MutableSet<Int>>()

    private val sexIndex = ConcurrentHashMap<Char, MutableSet<Int>>()

    private val fnameIndex = ConcurrentHashMap<Int, MutableSet<Int>>()
    private val fnameNullIndex = concurrentHashSet<Int>()

    private val snameIndex = ConcurrentHashMap<Int, MutableSet<Int>>()
    private val snameNullIndex = concurrentHashSet<Int>()

    private val emailDomainIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val emailIndex = Array<AirConcurrentMap<String, Int>>(36) { AirConcurrentMap() }

    private val phoneCodeIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val phoneNullIndex = concurrentHashSet<Int>()

    private val countryIndex = ConcurrentHashMap<Int, MutableSet<Int>>()
    private val countryNullIndex = concurrentHashSet<Int>()

    private val cityIndex = ConcurrentHashMap<Int, MutableSet<Int>>()
    private val cityNullIndex = concurrentHashSet<Int>()

    private val birthIndex = AirConcurrentMap<Long, Int>()

    private val joinedIndex = AirConcurrentMap<Long, MutableCollection<Int>>()

    private val interestIndex = ConcurrentHashMap<Int, MutableSet<Int>>()

    private val likeIndex0_250 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex250_500 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex500_750 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex750_1000 = Array<MutableCollection<Int>>(250_000) { ArrayList(30) }
    private val likeIndex1000_1300 = Array<MutableCollection<Int>>(300_000) { ArrayList(30) }
    private val likeIndex1300 = ConcurrentHashMap<Int, MutableCollection<Int>>(100_000)

    private val premiumIndex = ConcurrentHashMap<Int, Boolean>(700_000)

    //normalization entities
    private val idCounter = AtomicInteger()
    private val cities = ConcurrentHashMap<String, Int>()
    private val countries = ConcurrentHashMap<String, Int>()
    private val interests = ConcurrentHashMap<String, Int>()
    private val statuses = ConcurrentHashMap<String, Int>()
    private val fnames = ConcurrentHashMap<String, Int>()
    private val snames = ConcurrentHashMap<String, Int>()

    private var ids = (1_700_000 downTo 0)

    @Volatile
    private var emailBuckets = mapOf<Int, Set<Int>>()

    init {
        fixedRateTimer("", true, 10_000, 60_000) {
            try {
                emailBuckets = emailIndex.mapIndexed { i, map -> i to map.values.toSet() }.toMap()
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    override fun addAccount(account: Account) {
        withLockById(account.id) {

            check(getAccountByIndex(account.id) == null) { "User ${account.id} already exists" }
            measureTimeAndReturnResult("id index") {
                val innerAccount = InnerAccount(
                    statuses.computeIfAbsent(account.status) { idCounter.incrementAndGet() },
                    account.email,
                    if (account.sex == 'm') 0 else 1,
                    account.fname?.let { fnames.computeIfAbsent(it) { idCounter.incrementAndGet() } },
                    account.sname?.let { snames.computeIfAbsent(it) { idCounter.incrementAndGet() } },
                    account.city?.let { cities.computeIfAbsent(it) { idCounter.incrementAndGet() } },
                    account.country?.let { countries.computeIfAbsent(it) { idCounter.incrementAndGet() } },
                    account.birth,
                    account.phone,
                    account.premium,
                    account.interests?.map { interests.computeIfAbsent(it) { idCounter.incrementAndGet() } }
                )
                when {
                    account.id < 250_000 -> accounts0_250[account.id] = innerAccount
                    account.id in 250_000 until 500_000 -> accounts250_500[account.id - 250_000] = innerAccount
                    account.id in 500_000 until 750_000 -> accounts500_750[account.id - 500_000] = innerAccount
                    account.id in 750_000 until 1_000_000 -> accounts750_1000[account.id - 750_000] = innerAccount
                    account.id in 1_000_000 until 1_300_000 -> accounts1000_1300[account.id - 1_000_000] = innerAccount
                    else -> accounts1300[account.id] = innerAccount
                }
            }

            measureTimeAndReturnResult("sex index:") {
                sexIndex.computeIfAbsent(account.sex) { concurrentHashSet(600_000) }.add(account.id)
            }

            measureTimeAndReturnResult("status index:") {
                statusIndex.computeIfAbsent(statuses[account.status]!!) { concurrentHashSet(300_000) }.add(account.id)
            }

            measureTimeAndReturnResult("fname index:") {
                account.fname?.let {
                    val collection = fnameIndex.computeIfAbsent(fnames[it]!!) { concurrentHashSet() }
                    collection.add(account.id)
                } ?: fnameNullIndex.add(account.id)
            }

            measureTimeAndReturnResult("sname index:") {
                account.sname?.let {
                    val collection = snameIndex.computeIfAbsent(snames[it]!!) { concurrentHashSet() }
                    collection.add(account.id)
                } ?: snameNullIndex.add(account.id)
            }

            account.email.let { email ->
                measureTimeAndReturnResult("email domain index:") {
                    val domain = email.split("@").let { parsedEmail -> parsedEmail[parsedEmail.size - 1] }
                    val domainCollection = emailDomainIndex.computeIfAbsent(domain) { concurrentHashSet(100_000) }
                    domainCollection.add(account.id)
                }
                measureTimeAndReturnResult("email index:") {
                    addEmailToIndex(email, account.id)
                }
            }

            measureTimeAndReturnResult("phone index:") {
                account.phone?.let { phone ->
                    val code = parsePhoneCode(phone)
                    val codeCollection = phoneCodeIndex.computeIfAbsent(code) { concurrentHashSet() }
                    codeCollection.add(account.id)
                } ?: phoneNullIndex.add(account.id)
            }

            measureTimeAndReturnResult("country index:") {
                account.country?.let {
                    val collection = countryIndex.computeIfAbsent(countries[it]!!) { concurrentHashSet() }
                    collection.add(account.id)
                } ?: countryNullIndex.add(account.id)
            }

            measureTimeAndReturnResult("city index:") {
                account.city?.let {
                    val collection = cityIndex.computeIfAbsent(cities[it]!!) { concurrentHashSet() }
                    collection.add(account.id)
                } ?: cityNullIndex.add(account.id)
            }

            measureTimeAndReturnResult("birth index:") {
                birthIndex[account.birth] = account.id
            }

            measureTimeAndReturnResult("joined index:") {
                val joinedBucket = joinedIndex.computeIfAbsent(account.joined) { ArrayList(200) }
                synchronized(joinedBucket) {
                    joinedBucket.add(account.id)
                }
            }

            measureTimeAndReturnResult("interest index:") {
                (account.interests ?: emptyList()).forEach {
                    val collection = interestIndex.computeIfAbsent(interests[it]!!) { concurrentHashSet(15_000) }
                    collection.add(account.id)
                }
            }

            measureTimeAndReturnResult("like index:") {
                (account.likes ?: emptyList()).forEach { (likeIdInt, _) ->
                    val likeId = likeIdInt!!
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
            }

            measureTimeAndReturnResult("premium index:") {
                account.premium?.let { (start, finish) ->
                    premiumIndex.computeIfAbsent(account.id) { now() in (start until finish) }
                }
            }
        }
    }

    override fun filter(filterRequest: FilterRequest): List<MutableMap<String, Any?>> {
        val filters = mutableListOf<Set<Int>>()
        filterRequest.email?.let { (domain, lt, gt) ->
            if (domain != null) filters.add(emailDomainIndex[domain] ?: emptySet())
            if (lt != null || gt != null) {
                filters.add(getByEmailLtGt(lt, gt))
            }
        }

        filterRequest.fname?.let { (eq, any, _) ->
            if (eq != null) filters.add(fnames[eq]?.let { fnameIndex[it] } ?: emptySet())
            if (any != null) filters.add(any.flatMap { fnames[eq]?.let { fnameIndex[it] } ?: emptySet<Int>() }.toSet())
        }

        filterRequest.sname?.let { (eq, starts, _) ->
            if (starts != null) {
                filters.add(snames.filter { it.key.startsWith(starts) }.flatMap {
                    snameIndex[it.value] ?: emptyList<Int>()
                }.toSet())
            }
            if (eq != null) filters.add(snames[eq]?.let { snameIndex[it] } ?: emptySet())
        }

        filterRequest.phone?.let { (eq, _) ->
            if (eq != null) filters.add(phoneCodeIndex[eq] ?: emptySet())
        }

        filterRequest.country?.let { (eq, _) ->
            if (eq != null) filters.add(countries[eq]?.let { countryIndex[it] } ?: emptySet())
        }

        filterRequest.city?.let { (eq, any, _) ->
            if (eq != null) filters.add(countries[eq]?.let { countryIndex[it] } ?: emptySet())
            if (any != null) filters.add(any.flatMap {
                countries[it]?.let { countryIndex[it] } ?: emptySet<Int>()
            }.toSet())
        }

        filterRequest.birth?.let { (lt, gt, year) ->
            when {
                lt != null && gt != null -> birthIndex.subMap(gt, lt)
                lt != null -> birthIndex.headMap(lt)
                gt != null -> birthIndex.tailMap(gt)
                else -> null
            }?.values?.let { filters.add(it.toSet()) }

            if (year != null) filters.add(queryByYear(year, birthIndex).toSet())
        }

        filterRequest.interests?.let { (contains, any) ->
            contains?.let { interests ->
                interests.map { this.interests[it]?.let { interestIndex[it] } ?: emptyList<Int>() }
                    .reduce { l, r -> l.intersect(r) }
            }?.let { filters.add(it.toSet()) }
            any?.let { interests ->
                interests.flatMap { this.interests[it]?.let { interestIndex[it] } ?: emptyList<Int>() }
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
            .filter { id -> filters.none { !it.contains(id) } }
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
                    val eqDecision = eq?.let { statusIndex[statuses[it]]!!.contains(id) } ?: true
                    val neqDecision = neq?.let { !statusIndex[statuses[it]]!!.contains(id) } ?: true
                    neqDecision && eqDecision
                } ?: true
            }
            .take(filterRequest.limit)
            .map { id ->
                val innerAccount = getAccountByIndex(id)!!
                val resultObj = mutableMapOf<String, Any?>("id" to id, "email" to innerAccount.email)

                filterRequest.sex?.let { resultObj["sex"] = filterRequest.sex.eq }
                filterRequest.status?.let {
                    resultObj["status"] = statuses.entries.firstOrNull { it.value == innerAccount.status }?.key
                }
                filterRequest.fname?.let {
                    resultObj["fname"] = fnames.entries.firstOrNull { it.value == innerAccount.fname }?.value
                }
                filterRequest.sname?.let {
                    resultObj["sname"] = snames.entries.firstOrNull { it.value == innerAccount.sname }?.value
                }
                filterRequest.phone?.let { resultObj["phone"] = innerAccount.phone }
                filterRequest.country?.let {
                    resultObj["country"] = countries.entries.firstOrNull { it.value == innerAccount.country }?.value
                }
                filterRequest.city?.let {
                    resultObj["city"] = cities.entries.firstOrNull { it.value == innerAccount.city }?.value
                }
                filterRequest.birth?.let { resultObj["birth"] = innerAccount.birth }
                filterRequest.premium?.let { resultObj["premium"] = innerAccount.premium }

                resultObj
            }
            .toList()
    }

    override fun group(groupRequest: GroupRequest): List<Group> {
        return listOf()

        val filters = mutableListOf<Set<Int>>()

        groupRequest.sname?.let { snames[it]?.let { snameIndex[it] } ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.fname?.let { fnames[it]?.let { fnameIndex[it] } ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.sex?.let { sexIndex[it] }?.let { filters.add(it) }

        groupRequest.country?.let { countries[it]?.let { countryIndex[it] } ?: emptySet<Int>() }
            ?.let { filters.add(it) }
        groupRequest.city?.let { cities[it]?.let { cityIndex[it] } ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.status?.let { statuses[it]?.let { statusIndex[it] } ?: emptySet<Int>() }?.let { filters.add(it) }
        groupRequest.interests?.let { interests[it]?.let { interestIndex[it] } ?: emptySet<Int>() }
            ?.let { filters.add(it) }

        groupRequest.birthYear?.let { queryByYear(it, birthIndex) }?.let { filters.add(it.toSet()) }
        groupRequest.joinedYear?.let { queryComplexByYear(it, joinedIndex) }?.let { filters.add(it.toSet()) }
        groupRequest.likes?.let { getLikesByIndex(it).toSet() }?.let { filters.add(it) }

        data class GroupTemp(val sex: Int?, val status: Int?, val interest: Int?, val country: Int?, val city: Int?)

        val useSex = groupRequest.keys.contains("sex")
        val useStatus = groupRequest.keys.contains("status")
        val useInterest = groupRequest.keys.contains("interests")
        val useCountry = groupRequest.keys.contains("country")
        val useCity = groupRequest.keys.contains("city")

        val listWithNull = listOf(null)

        return ids.asSequence()
            .filter { id -> filters.none { !it.contains(id) } }
            .mapNotNull { getAccountByIndex(it) }
            .flatMap { acc ->
                if (useInterest) {
                    (acc.interests ?: listWithNull).map { interest ->
                        GroupTemp(
                            if (useSex) acc.sex else null,
                            if (useStatus) acc.status else null,
                            interest,
                            if (useCountry) acc.country else null,
                            if (useCity) acc.city else null
                        )
                    }.asSequence()
                } else sequenceOf(
                    GroupTemp(
                        if (useSex) acc.sex else null,
                        if (useStatus) acc.status else null,
                        null,
                        if (useCountry) acc.country else null,
                        if (useCity) acc.city else null
                    )
                )
            }.groupingBy { it }.eachCount()
            .entries
            //todo manual sort
            .sortedBy { it.component2() }
            .take(groupRequest.limit)
            .map { (group, count) ->
                Group(
                    count,
                    if (group.sex == 0) 'm' else 'f',
                    group.status?.let { groupStatus -> statuses.entries.firstOrNull { it.value == groupStatus }?.key },
                    group.interest?.let { groupInterest ->
                        interests.entries.firstOrNull { it.value == groupInterest }?.key
                    },
                    group.country?.let { groupCountry ->
                        countries.entries.firstOrNull { it.value == groupCountry }?.key
                    },
                    group.city?.let { groupCity -> cities.entries.firstOrNull { it.value == groupCity }?.key }
                )
            }
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

    private fun addEmailToIndex(email: String, id: Int) {
        emailIndex[getLexIndex(email[0])].compute(email) { _, existingId ->
            if (existingId != null) throw RuntimeException("email $email already binded")
            else id
        }
    }

    private fun getByEmailLtGt(lt: String?, gt: String?): Set<Int> {
        check(lt != null || gt != null)
        val ltFirst = lt?.let { getLexIndex(lt[0]) } ?: 35
        val gtFirst = gt?.let { getLexIndex(gt[0]) } ?: 0

        val localBuckets = this.emailBuckets
        return when {
            ltFirst < gtFirst -> emptySet()
            ltFirst == gtFirst -> emailIndex[ltFirst].subMap(gt, true, lt, true).values.toSet()
            else -> (gtFirst + 1 until ltFirst)
                .map { localBuckets[it]!! }
                .plusElement(if (lt != null) emailIndex[ltFirst].headMap(lt).values.toSet() else localBuckets[ltFirst]!!)
                .plusElement(if (gt != null) emailIndex[gtFirst].tailMap(gt).values.toSet() else localBuckets[gtFirst]!!)
                .let { CompositeSet(it) }
        }
    }

    private fun getLexIndex(char: Char) = when (char) {
        '0' -> 0
        '1' -> 1
        '2' -> 2
        '3' -> 3
        '4' -> 4
        '5' -> 5
        '6' -> 6
        '7' -> 7
        '8' -> 8
        '9' -> 9
        'a' -> 10
        'b' -> 11
        'c' -> 12
        'd' -> 13
        'e' -> 14
        'f' -> 15
        'g' -> 16
        'h' -> 17
        'i' -> 18
        'j' -> 19
        'k' -> 20
        'l' -> 21
        'm' -> 22
        'n' -> 23
        'o' -> 24
        'p' -> 25
        'q' -> 26
        'r' -> 27
        's' -> 28
        't' -> 29
        'u' -> 30
        'v' -> 31
        'w' -> 32
        'x' -> 33
        'y' -> 34
        'z' -> 35
        else -> throw RuntimeException("Unknown char $char")
    }
}
