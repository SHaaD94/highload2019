package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.FilterRequest
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
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

    private val statusIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

    private val sexIndex = ConcurrentHashMap<Char, Array<ArrayList<Int>>>()

    private val fnameIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

    private val snameIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

    private val emailDomainIndex = ConcurrentHashMap<String, Array<ArrayList<Int>>>()
    private val emailIndex = Array(36) { ConcurrentHashMap<String, Int>() }

    private val phoneCodeIndex = Array(1000) { Array(20) { ArrayList<Int>() } }

    private val countryIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

    private val cityIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

    private val birthIndex = Array(100) { Array(20) { ArrayList<Int>() } }

    private val joinedIndex = Array(10) { Array(20) { ArrayList<Int>() } }

    private val interestIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

    private val likeIndex0_250 = Array<ArrayList<Int>>(250_000) { ArrayList() }
    private val likeIndex250_500 = Array<ArrayList<Int>>(250_000) { ArrayList() }
    private val likeIndex500_750 = Array<ArrayList<Int>>(250_000) { ArrayList() }
    private val likeIndex750_1000 = Array<ArrayList<Int>>(250_000) { ArrayList() }
    private val likeIndex1000_1300 = Array<ArrayList<Int>>(300_000) { ArrayList() }
    private val likeIndex1300 = ConcurrentHashMap<Int, ArrayList<Int>>(100_000)

    //normalization entities
    private val idCounter = AtomicInteger()
    private val cities = ConcurrentHashMap<String, Int>()
    private val citiesInv = ConcurrentHashMap<Int, String>()
    private val countries = ConcurrentHashMap<String, Int>()
    private val countriesInv = ConcurrentHashMap<Int, String>()
    private val interests = ConcurrentHashMap<String, Int>()
    private val interestsInv = ConcurrentHashMap<Int, String>()
    private val statuses = ConcurrentHashMap<String, Int>()
    private val statusesInv = ConcurrentHashMap<Int, String>()
    private val fnames = ConcurrentHashMap<String, Int>()
    private val fnamesInv = ConcurrentHashMap<Int, String>()
    private val snames = ConcurrentHashMap<String, Int>()
    private val snamesInv = ConcurrentHashMap<Int, String>()

    @Volatile
    private var ids = (1_700_000 downTo 0)

    @Volatile
    private var maxId = 0

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
        withLockById(account.id!!) {
            check(getAccountByIndex(account.id) == null) { "User ${account.id} already exists" }

            measureTimeAndReturnResult("id index") {
                val innerAccount = InnerAccount(
                    account.id,
                    writeNormalizationIndex(statuses, statusesInv, account.status),
                    account.email,
                    if (account.sex == 'm') 0 else 1,
                    account.fname?.let { writeNormalizationIndex(fnames, fnamesInv, it) },
                    account.sname?.let { writeNormalizationIndex(snames, snamesInv, it) },
                    account.city?.let { writeNormalizationIndex(cities, citiesInv, it) },
                    account.country?.let { writeNormalizationIndex(countries, countriesInv, it) },
                    account.birth,
                    account.phone,
                    account.premium,
                    account.interests?.map { writeNormalizationIndex(interests, interestsInv, it) }
                    //account.likes?.map {  InnerLike(it) }
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
                val collection = sexIndex.computeIfAbsent(account.sex) { Array(20) { ArrayList<Int>() } }
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }

            measureTimeAndReturnResult("status index:") {
                val collection =
                    statusIndex.computeIfAbsent(statuses[account.status]!!) { Array(20) { ArrayList<Int>() } }
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }

            measureTimeAndReturnResult("fname index:") {
                account.fname?.let {
                    val collection = fnameIndex.computeIfAbsent(fnames[it]!!) { Array(20) { ArrayList<Int>() } }
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
                }
            }

            measureTimeAndReturnResult("sname index:") {
                account.sname?.let {
                    val collection = snameIndex.computeIfAbsent(snames[it]!!) { Array(20) { ArrayList<Int>() } }
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
                }
            }

            account.email.let { email ->
                measureTimeAndReturnResult("email domain index:") {
                    val domain = email.split("@").let { parsedEmail -> parsedEmail[parsedEmail.size - 1] }
                    val collection = emailDomainIndex.computeIfAbsent(domain) { Array(20) { ArrayList<Int>() } }
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
                }
                measureTimeAndReturnResult("email index:") {
                    addEmailToIndex(email, account.id)
                }
            }

            measureTimeAndReturnResult("phone index:") {
                account.phone?.let { phone ->
                    val code = parsePhoneCode(phone)
                    val collection = phoneCodeIndex[code.toInt()]
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
                }
            }

            measureTimeAndReturnResult("country index:") {
                account.country?.let {
                    val collection = countryIndex.computeIfAbsent(countries[it]!!) { Array(20) { ArrayList<Int>() } }
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
                }
            }

            measureTimeAndReturnResult("city index:") {
                account.city?.let {
                    val collection = cityIndex.computeIfAbsent(cities[it]!!) { Array(20) { ArrayList<Int>() } }
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
                }
            }

            measureTimeAndReturnResult("birth index:") {
                val collection = birthIndex[getYear(account.birth) - 1920]
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }

            measureTimeAndReturnResult("joined index:") {
                val collection = joinedIndex[getYear(account.joined) - 2010]
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }

            measureTimeAndReturnResult("interest index:") {
                (account.interests ?: emptyList()).forEach {
                    val collection = interestIndex.computeIfAbsent(interests[it]!!) { Array(20) { ArrayList<Int>() } }
                    addToSortedCollection(getIdBucket(account.id, collection), account.id)
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

                    addToSortedCollection(collection, account.id)
                }
            }

        }
        if (account.id > maxId) {
            synchronized(maxId) {
                maxId = account.id
                ids = (maxId downTo 0)
            }
        }
    }

    private fun writeNormalizationIndex(
        index: ConcurrentHashMap<String, Int>,
        invIndex: ConcurrentHashMap<Int, String>,
        property: String
    ): Int {
        return index.computeIfAbsent(property) {
            val id = idCounter.incrementAndGet()
            invIndex[id] = property
            id
        }
    }

    override fun filter(filterRequest: FilterRequest): List<MutableMap<String, Any?>> {
        val indexes = mutableListOf<Iterator<Int>>()
        filterRequest.email?.let { (domain, lt, gt) ->
            if (domain != null) indexes.add(getIterator(emailDomainIndex[domain]))
        }
        filterRequest.sex?.let { (eq) ->
            indexes.add(getIterator(sexIndex[eq]))
        }
        filterRequest.status?.let { (eq, neq) ->
            if (eq != null) {
                indexes.add(getIterator(statusIndex[statuses[eq]]))
            }
            if (neq != null) {
                statuses.keys().asSequence().filter { it != neq }.map { statuses[it]!! }
                    .map { getIterator(statusIndex[it]!!) }
                    .toList()
                    .let { indexes.add(joinIterators(it)) }
            }
        }

        filterRequest.fname?.let { (eq, any, _) ->
            if (eq != null) indexes.add(getIterator(fnames[eq]?.let { fnameIndex[it] }))
            if (any != null) indexes.add(any.map {
                getIterator(fnames[it]?.let { fnameIndex[it] })
            }.let { joinIterators(it) })
        }

        filterRequest.sname?.let { (eq, starts, _) ->
            if (starts != null) {
                indexes.add(snames.filter { it.key.startsWith(starts) }.map {
                    getIterator(snameIndex[it.value])
                }.let { joinIterators(it) })
            }
            if (eq != null) indexes.add(getIterator(snames[eq]?.let { snameIndex[it] }))
        }

        filterRequest.phone?.let { (eq, _) ->
            if (eq != null) indexes.add(getIterator(phoneCodeIndex[eq.toInt()]))
        }

        filterRequest.country?.let { (eq, _) ->
            if (eq != null) indexes.add(getIterator(countries[eq]?.let { countryIndex[it] }))
        }

        filterRequest.city?.let { (eq, any, _) ->
            if (eq != null) indexes.add(getIterator(cities[eq]?.let { cityIndex[it] }))
            if (any != null) indexes.add(any.map {
                getIterator(cities[it]?.let { cityIndex[it] })
            }.let { joinIterators(it) })
        }

        filterRequest.birth?.let { (lt, gt, year) ->
            if (lt != null || gt != null) {
                val ltY = checkBirthYear(lt?.let { getYear(lt) - 1920 } ?: 99)
                val gtY = checkBirthYear(gt?.let { getYear(gt) - 1920 } ?: 0)


                val iterators = (gtY + 1 until ltY).asSequence()
                    .map { birthIndex[it] }
                    .filter { it.any { !it.isEmpty() } }
                    .map { getIterator(it) }
                    .toMutableList()
                if (birthIndex[ltY].any { !it.isEmpty() }) {
                    val iter = getIterator(birthIndex[ltY])
                    if (lt == null) iter else {
                        iter.asSequence().filter { getAccountByIndex(it)!!.birth <= lt }
                            .iterator()
                    }.let { iterators.add(it) }
                }
                if (birthIndex[gtY].any { !it.isEmpty() }) {
                    val iter = getIterator(birthIndex[gtY])
                    if (gt == null) iter else {
                        iter.asSequence().filter { getAccountByIndex(it)!!.birth >= gt }
                            .iterator()
                    }.let { iterators.add(it) }
                }
                indexes.add(joinIterators(iterators))
            }

            if (year != null) indexes.add(getIterator(birthIndex[checkBirthYear(year - 1920)]))
        }

        filterRequest.interests?.let { (contains, any) ->
            contains?.let { interests ->
                interests.asSequence()
                    .map { getIterator(this.interests[it]?.let { interestIndex[it] }) }
                    .forEach { indexes.add(it) }
            }
            any?.let { interests ->
                interests.map { getIterator(this.interests[it]?.let { interestIndex[it] }) }
            }?.let { indexes.add(joinIterators(it)) }
        }

        filterRequest.likes?.let { (contains) ->
            contains?.let { likes ->
                likes.asSequence().map { getLikesByIndex(it).iterator() }.forEach { indexes.add(it) }
            }
        }

        val sequence = if (indexes.isEmpty()) ids.asSequence() else generateSequenceFromIndexes(indexes)

        return sequence
            .mapNotNull { getAccountByIndex(it) }
            .filter { acc ->
                if (filterRequest.email != null) {
                    val ltFilter = if (filterRequest.email.lt != null) {
                        filterRequest.email.lt >= acc.email
                    } else true

                    val gtFilter = if (filterRequest.email.gt != null) {
                        filterRequest.email.gt <= acc.email
                    } else true
                    ltFilter && gtFilter
                } else true
            }
            .filter { id ->
                filterByNull(filterRequest.fname?.nill, id.fname) &&
                        filterByNull(filterRequest.sname?.nill, id.sname) &&
                        filterByNull(filterRequest.phone?.nill, id.phone) &&
                        filterByNull(filterRequest.country?.nill, id.country) &&
                        filterByNull(filterRequest.city?.nill, id.city) &&
                        filterByNull(filterRequest.premium?.nill, id.premium)
            }
            .filter { acc ->
                filterRequest.premium?.let { (now, _) ->
                    if (now != null) acc.premium?.let {
                        System.currentTimeMillis() / 1000 in (it.start..it.finish)
                    } == true else true
                } ?: true
            }
            .take(filterRequest.limit)
            .map { innerAccount ->
                val resultObj = mutableMapOf("id" to innerAccount.id, "email" to innerAccount.email)

                filterRequest.sex?.let { resultObj["sex"] = filterRequest.sex.eq }
                filterRequest.status?.let {
                    resultObj["status"] = statusesInv[innerAccount.status]
                }
                filterRequest.fname?.let {
                    resultObj["fname"] = innerAccount.fname?.let { fnamesInv[it] }
                }
                filterRequest.sname?.let {
                    resultObj["sname"] = innerAccount.sname?.let { snamesInv[it] }
                }
                filterRequest.phone?.let {
                    resultObj["phone"] = innerAccount.phone
                }
                filterRequest.country?.let {
                    resultObj["country"] = innerAccount.country?.let { countriesInv[it] }
                }
                filterRequest.city?.let {
                    resultObj["city"] = innerAccount.city?.let { citiesInv[it] }
                }
                filterRequest.birth?.let { resultObj["birth"] = innerAccount.birth }
                filterRequest.premium?.let { resultObj["premium"] = innerAccount.premium }

                resultObj
            }
            .toList()
    }

    override fun group(groupRequest: GroupRequest): List<Group> {
        val indexes = mutableListOf<Iterator<Int>>()

        groupRequest.sname?.let { snames[it] }?.let { snameIndex[it] }?.let { indexes.add(getIterator(it)) }
        groupRequest.fname?.let { fnames[it] }?.let { fnameIndex[it] }?.let { indexes.add(getIterator(it)) }
        groupRequest.sex?.let { sexIndex[it] }?.let { indexes.add(getIterator(it)) }

        groupRequest.country?.let { countries[it]?.let { countryIndex[it] } }?.let { indexes.add(getIterator(it)) }
        groupRequest.city?.let { cities[it]?.let { cityIndex[it] } }?.let { indexes.add(getIterator(it)) }
        groupRequest.status?.let { statuses[it] }?.let { statusIndex[it] }?.let { indexes.add(getIterator(it)) }
        groupRequest.interests?.let { interests[it] }?.let { interestIndex[it] }?.let { indexes.add(getIterator(it)) }

        groupRequest.birthYear?.let { year ->
            val mappedYear = checkBirthYear(year - 1920)
            birthIndex[mappedYear]
        }?.let { indexes.add(getIterator(it)) }
        groupRequest.joinedYear?.let { year ->
            val mappedYear = (year - 2010).let {
                when {
                    it > 9 -> 9
                    it < 0 -> 0
                    else -> it
                }
            }
            joinedIndex[mappedYear]
        }?.let { indexes.add(getIterator(it)) }
        groupRequest.likes?.let { getLikesByIndex(it) }?.let { indexes.add(it.iterator()) }

        data class GroupTemp(val sex: Int?, val status: Int?, val interest: Int?, val country: Int?, val city: Int?)

        val useSex = groupRequest.keys.contains("sex")
        val useStatus = groupRequest.keys.contains("status")
        val useInterest = groupRequest.keys.contains("interests")
        val useCountry = groupRequest.keys.contains("country")
        val useCity = groupRequest.keys.contains("city")

        val listWithNull = listOf(null)

        val sequence = if (indexes.isEmpty()) ids.asSequence() else generateSequenceFromIndexes(indexes)

        val tempGroups = sequence
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
            .entries.toList()

        val resultGroups = ArrayList<Pair<GroupTemp, Int>>()
        var iterations = groupRequest.limit
        val checkedEntries = mutableListOf<Any>()

        fun getComparator(pos: Int, pair: Map.Entry<GroupTemp, Int>): String? =
            when (groupRequest.keys.getOrNull(pos)) {
                "sex" -> int2Sex(pair.key.sex).toString()
                "status" -> pair.key.status?.let { statusesInv[it] }
                "interests" -> pair.key.interest?.let { interestsInv[it] }
                "country" -> pair.key.country?.let { countriesInv[it] }
                "city" -> pair.key.city?.let { citiesInv[it] }
                else -> null
            }

        val comparator = compareBy<Map.Entry<GroupTemp, Int>>({ it.value },
            { getComparator(0, it) }, { getComparator(1, it) }, { getComparator(2, it) },
            { getComparator(3, it) }, { getComparator(4, it) })
        while (iterations != 0) {
            val element =
                tempGroups
                    .asSequence()
                    .filter { e -> checkedEntries.none { e === it } }
                    .let { groups ->
                        if (groupRequest.order == 1) groups.minWith(comparator) else groups.maxWith(comparator)
                    } ?: break
            checkedEntries.add(element)
            resultGroups.add(element.key to element.value)
            iterations--
        }

        return resultGroups.map {
            Group(
                it.second,
                when {
                    it.first.sex == 0 -> 'm'
                    it.first.sex == 1 -> 'f'
                    else -> null
                },
                it.first.status?.let { statusesInv[it] },
                it.first.interest?.let { interestsInv[it] },
                it.first.country?.let { countriesInv[it] },
                it.first.city?.let { citiesInv[it] }
            )
        }
    }


    private fun checkBirthYear(year: Int): Int {
        return when {
            year > 99 -> 99
            year < 0 -> 0
            else -> year
        }

    }

    private fun int2Sex(int: Int?) = when {
        int == 0 -> 'm'
        int == 1 -> 'f'
        else -> null
    }

    private fun filterByNull(nill: Boolean?, property: Any?) =
        if (nill != null) {
            when (nill) {
                true -> property != null
                false -> property == null
            }
        } else true

    private fun withLockById(id: Int, block: () -> Unit) = synchronized(id) { block() }

    private fun getLikesByIndex(likeId: Int): ArrayList<Int> =
        when {
            likeId < 250_000 -> likeIndex0_250[likeId]
            likeId in 250_000 until 500_000 -> likeIndex250_500[likeId - 250_000]
            likeId in 500_000 until 750_000 -> likeIndex500_750[likeId - 500_000]
            likeId in 750_000 until 1_000_000 -> likeIndex750_1000[likeId - 750_000]
            likeId in 1_000_000 until 1_300_000 -> likeIndex1000_1300[likeId - 1_000_000]

            else -> likeIndex1300[likeId]!!
        }

    fun getAccountByIndex(id: Int): InnerAccount? = when {
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

//    private fun getByEmailLtGt(lt: String?, gt: String?): Set<Int> {
//        check(lt != null || gt != null)
//        val ltFirst = lt?.let { getLexIndex(lt[0]) } ?: 35
//        val gtFirst = gt?.let { getLexIndex(gt[0]) } ?: 0
//
//        val localBuckets = this.emailBuckets
//        return when {
//            ltFirst < gtFirst -> emptySet()
//            ltFirst == gtFirst -> emailIndex[ltFirst].subMap(gt, true, lt, true).values.toSet()
//            else -> (gtFirst + 1 until ltFirst)
//                .map { localBuckets[it]!! }
//                .plusElement(if (lt != null) emailIndex[ltFirst].headMap(lt).values.toSet() else localBuckets[ltFirst]!!)
//                .plusElement(if (gt != null) emailIndex[gtFirst].tailMap(gt).values.toSet() else localBuckets[gtFirst]!!)
//                .let { CompositeSet(it) }
//        }
//    }

    private fun getYear(timestamp: Int): Int {
        return LocalDateTime.ofEpochSecond(timestamp.toLong(), 0, moscowTimeZone).year
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

    private fun getIterator(array: Array<ArrayList<Int>>?): Iterator<Int> {
        array ?: return emptyIterator()
        val resultIterators = array.filter { !it.isEmpty() }.map { it.iterator() }
        return if (resultIterators.isEmpty()) {
            emptyIterator()
        } else {
            joinIterators(resultIterators)
        }
    }

    private fun getIdBucket(id: Int, array: Array<ArrayList<Int>>): ArrayList<Int> = when {
        id < 100_000 -> 0
        id in 100_000 until 200_000 -> 1
        id in 200_000 until 300_000 -> 2
        id in 300_000 until 400_000 -> 3
        id in 400_000 until 500_000 -> 4
        id in 500_000 until 600_000 -> 5
        id in 600_000 until 700_000 -> 6
        id in 700_000 until 800_000 -> 7
        id in 800_000 until 900_000 -> 8
        id in 900_000 until 1000_000 -> 9
        id in 1000_000 until 1100_000 -> 10
        id in 1100_000 until 1200_000 -> 11
        id in 1200_000 until 1300_000 -> 12
        id in 1300_000 until 1400_000 -> 13
        id in 1400_000 until 1500_000 -> 14
        id in 1500_000 until 1600_000 -> 15
        id in 1600_000 until 1700_000 -> 16
        id in 1700_000 until 1800_000 -> 17
        id in 1800_000 until 1900_000 -> 18
        else -> 19
    }.let { array[it] }

    private fun addToSortedCollection(list: ArrayList<Int>, id: Int?) {
        synchronized(list) {
            val closest = searchClosest(id!!, list)

            list.add(closest, id)
        }
    }

    private fun searchClosest(target: Int, nums: ArrayList<Int>): Int {
        var i = 0
        var j = nums.size - 1

        while (i <= j) {
            val mid = (i + j) / 2

            if (target < nums[mid]) {
                i = mid + 1
            } else if (target > nums[mid]) {
                j = mid - 1
            } else {
                return mid
            }
        }

        return i
    }

}


