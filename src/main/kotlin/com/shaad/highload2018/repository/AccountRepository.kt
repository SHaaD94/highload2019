package com.shaad.highload2018.repository

import com.google.common.primitives.UnsignedBytes
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.FilterRequest
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayList

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): Sequence<InnerAccount>
    fun group(groupRequest: GroupRequest): Sequence<Group>
    fun recommend(id: Int?, city: String?, country: String?, limit: Int): Sequence<InnerAccount>
}

class AccountRepositoryImpl : AccountRepository {
    @Volatile
    private var maxId = 0

    override fun addAccount(account: Account) {
        check(getAccountByIndex(account.id!!) == null) { "User ${account.id} already exists" }

        measureTimeAndReturnResult("id index") {
            val innerAccount = InnerAccount(
                account.id,
                writeNormalizationIndex(statuses, statusesInv, account.status),
                account.email.toByteArray(),
                if (account.sex == 'm') 0 else 1,
                account.fname?.let { writeNormalizationIndex(fnames, fnamesInv, it) },
                account.sname?.let { writeNormalizationIndex(snames, snamesInv, it) },
                account.city?.let { writeNormalizationIndex(cities, citiesInv, it) },
                account.country?.let { writeNormalizationIndex(countries, countriesInv, it) },
                account.birth,
                account.phone?.toByteArray(),
                account.premium?.start,
                account.premium?.finish,
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

        measureTimeAndReturnResult("premium index:") {
            if (account.premium != null && System.currentTimeMillis() / 1000 in (account.premium.start..account.premium.finish)) {
                addToSortedCollection(getIdBucket(account.id, premiumNowIndex), account.id)
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

        if (account.id > maxId) {
            synchronized(maxId) {
                if (account.id > maxId) {
                    maxId = account.id
                }
            }
        }
    }

    private val lexComparator = UnsignedBytes.lexicographicalComparator()
    override fun filter(filterRequest: FilterRequest): Sequence<InnerAccount> {
        val indexes = mutableListOf<Iterator<Int>>()
        filterRequest.email?.let { (domain, _, _) ->
            if (domain != null) indexes.add(emailDomainIndex[domain].getPartitionedIterator())
        }
        filterRequest.sex?.let { (eq) ->
            indexes.add(sexIndex[eq].getPartitionedIterator())
        }
        filterRequest.premium?.let { (now, _) ->
            if (now != null) {
                indexes.add(premiumNowIndex.getPartitionedIterator())
            }
        }
        filterRequest.status?.let { (eq, neq) ->
            if (eq != null) {
                indexes.add(statusIndex[statuses[eq]].getPartitionedIterator())
            }
            if (neq != null) {
                statuses.keys().asSequence().filter { it != neq }.map { statuses[it]!! }
                    .map { statusIndex[it]!!.getPartitionedIterator() }
                    .toList()
                    .let { indexes.add(joinIterators(it)) }
            }
        }

        filterRequest.fname?.let { (eq, any, _) ->
            if (eq != null) indexes.add(fnames[eq]?.let { fnameIndex[it] }.getPartitionedIterator())
            if (any != null) indexes.add(any.map {
                fnames[it]?.let { fnameIndex[it] }.getPartitionedIterator()
            }.let { joinIterators(it) })
        }

        filterRequest.sname?.let { (eq, starts, _) ->
            if (starts != null) {
                indexes.add(snames.filter { it.key.startsWith(starts) }.map {
                    snameIndex[it.value].getPartitionedIterator()
                }.let { joinIterators(it) })
            }
            if (eq != null) indexes.add(snames[eq]?.let { snameIndex[it] }.getPartitionedIterator())
        }

        filterRequest.phone?.let { (eq, _) ->
            if (eq != null) indexes.add(phoneCodeIndex[eq.toInt()].getPartitionedIterator())
        }

        filterRequest.country?.let { (eq, _) ->
            if (eq != null) indexes.add(countries[eq]?.let { countryIndex[it] }.getPartitionedIterator())
        }

        filterRequest.city?.let { (eq, any, _) ->
            if (eq != null) indexes.add(cities[eq]?.let { cityIndex[it] }.getPartitionedIterator())
            if (any != null) indexes.add(any.map {
                cities[it]?.let { cityIndex[it] }.getPartitionedIterator()
            }.let { joinIterators(it) })
        }

        filterRequest.birth?.let { (lt, gt, year) ->
            if (lt != null || gt != null) {
                val ltY = checkBirthYear(lt?.let { getYear(lt) - 1920 } ?: 99)
                val gtY = checkBirthYear(gt?.let { getYear(gt) - 1920 } ?: 0)


                val iterators = (gtY + 1 until ltY).asSequence()
                    .map { birthIndex[it] }
                    .map { it.getPartitionedIterator() }
                    .toMutableList()

                val ltIterator = birthIndex[ltY].getPartitionedIterator()
                if (lt == null) ltIterator else {
                    ltIterator.asSequence().filter {
                        val acc = getAccountByIndex(it)!!
                        acc.birth <= lt
                    }.iterator()
                }.let { iterators.add(it) }


                val gtIterator = birthIndex[gtY].getPartitionedIterator()
                if (gt == null) gtIterator else {
                    gtIterator.asSequence().filter {
                        val acc = getAccountByIndex(it)!!
                        acc.birth >= gt
                    }.iterator()
                }.let { iterators.add(it) }

                //todo improvements
                indexes.add(joinIterators(iterators))
            }

            if (year != null) indexes.add(birthIndex[checkBirthYear(year - 1920)].getPartitionedIterator())
        }

        filterRequest.interests?.let { (contains, any) ->
            contains?.let { containsInterests ->
                containsInterests.asSequence()
                    .map { interests[it]?.let { interestIndex[it] }.getPartitionedIterator() }
                    .forEach { indexes.add(it) }
            }
            any?.let { anyInterests ->
                anyInterests.map { interests[it]?.let { interestIndex[it] }.getPartitionedIterator() }
            }?.let { indexes.add(joinIterators(it)) }
        }

        filterRequest.likes?.let { (contains) ->
            contains?.let { likes ->
                likes.asSequence().map { getLikesByIndex(it)?.iterator() ?: emptyIterator() }
                    .forEach { indexes.add(it) }
            }
        }

        val sequence = if (indexes.isEmpty()) fullIdsSequence() else generateSequenceFromIndexes(indexes)

        return sequence
            .mapNotNull { getAccountByIndex(it) }
            .filter { acc ->
                if (filterRequest.email != null) {
                    val ltFilter = if (filterRequest.email.lt != null) {
                        lexComparator.compare(acc.email, filterRequest.email.lt) <= 0
                    } else true

                    val gtFilter = if (filterRequest.email.gt != null) {
                        lexComparator.compare(acc.email, filterRequest.email.gt) >= 0
                        true
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
                        filterByNull(filterRequest.premium?.nill, id.premiumStart)
            }
            .take(filterRequest.limit)
    }

    private val listWithNull = listOf(null)
    override fun group(groupRequest: GroupRequest): Sequence<Group> {
        val indexes = mutableListOf<Iterator<Int>>()

        groupRequest.sname?.let { snames[it] }?.let { snameIndex[it] }?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.fname?.let { fnames[it] }?.let { fnameIndex[it] }?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.sex?.let { sexIndex[it] }?.let { indexes.add(it.getPartitionedIterator()) }

        groupRequest.country?.let { countries[it]?.let { countryIndex[it] } }
            ?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.city?.let { cities[it]?.let { cityIndex[it] } }?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.status?.let { statuses[it] }?.let { statusIndex[it] }
            ?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.interests?.let { interests[it] }?.let { interestIndex[it] }
            ?.let { indexes.add(it.getPartitionedIterator()) }

        groupRequest.birthYear?.let { year -> birthIndex[checkBirthYear(year - 1920)] }
            ?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.joinedYear?.let { year ->
            val mappedYear = (year - 2010).let {
                when {
                    it > 9 -> 9
                    it < 0 -> 0
                    else -> it
                }
            }
            joinedIndex[mappedYear]
        }?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.likes?.let { getLikesByIndex(it) ?: emptyList<Int>() }?.let { indexes.add(it.iterator()) }

        val useSex = groupRequest.keys.contains("sex")
        val useStatus = groupRequest.keys.contains("status")
        val useInterest = groupRequest.keys.contains("interests")
        val useCountry = groupRequest.keys.contains("country")
        val useCity = groupRequest.keys.contains("city")

        val sequence = if (indexes.isEmpty()) fullIdsSequence() else generateSequenceFromIndexes(indexes)

        //sex->status->country->city->interests
        var entries = 0
        val map =
            HashMap<Int?, MutableMap<Int?, MutableMap<Int?, MutableMap<Int?, MutableMap<Int?, AtomicInteger>>>>>()
        sequence
            .mapNotNull { getAccountByIndex(it) }
            .forEach { acc ->
                val sexMap =
                    map.computeIfAbsent(if (useSex) acc.sex else null) { HashMap() }
                val statusMap =
                    sexMap.computeIfAbsent(if (useStatus) acc.status else null) { HashMap() }
                val countryMap =
                    statusMap.computeIfAbsent(if (useCountry) acc.country else null) { HashMap() }
                val cityMap =
                    countryMap.computeIfAbsent(if (useCity) acc.city else null) { HashMap() }

                val interests = if (useInterest) acc.interests ?: listWithNull else listWithNull
                interests.forEach { interest ->
                    cityMap.compute(interest) { _, value ->
                        if (value == null) {
                            entries++
                        }
                        val v = value ?: AtomicInteger()
                        v.incrementAndGet()
                        v
                    }
                }
            }

        val tempGroups = ArrayList<Group>(entries)
        map.entries.forEach { (sex, statusMap) ->
            statusMap.forEach { (status, countryMap) ->
                countryMap.forEach { (country, cityMap) ->
                    cityMap.forEach { city, interests ->
                        interests.forEach { interest, count ->
                            tempGroups.add(Group(sex, status, interest, country, city, count.get()))
                        }
                    }
                }
            }
        }

        val result = ArrayList<Group>(groupRequest.limit)
        var i = 0
        while (i < groupRequest.limit || i == tempGroups.size) {
            var resGroup: Group? = null
            var j = 0
            val c = tempGroups[j]
            elemLoop@ while (j < tempGroups.size) {
                if (resGroup == null) {
                    resGroup = c
                    j++
                    continue
                }
                if (resGroup != c) {
                    val countComparison = resGroup.count!! - c.count!!
                    if (result.contains(resGroup)) {
                        j++
                        continue
                    }
                    if (countComparison < 0 && groupRequest.order > 0) {
                        result.add(resGroup)
                    }
                    if (countComparison > 0 && groupRequest.order < 0) {
                        result.add(resGroup)
                    }

                    if (countComparison == 0) {
                        val propertyCounter = 0
                        propertyLoop@ while (propertyCounter < 5) {
                            val resString = getGroupComparingString(groupRequest, countComparison, resGroup!!)
                            if (resString == null) {
                                result.add(resGroup)
                                break@elemLoop
                            }
                            val cString = getGroupComparingString(groupRequest, countComparison, c)
                            if (cString == null) {
                                result.add(c)
                                break@elemLoop
                            }
                            if (cString < resString && groupRequest.order > 0) {
                                resGroup = c
                                break
                            }
                            if (cString > resString && groupRequest.order < 0) {
                                resGroup = c
                                break
                            }
                        }
                    }
                }
                j++
            }
            i++
        }

        return result.asSequence()
    }

    private fun getGroupComparingString(groupRequest: GroupRequest, pos: Int?, g: Group): String? =
        when (groupRequest.keys.getOrNull(pos!!)) {
            "sex" -> int2Sex(g.sex).toString()
            "status" -> g.status?.let { statusesInv[it] }
            "interests" -> g.interest?.let { interestsInv[it] }
            "country" -> g.country?.let { countriesInv[it] }
            "city" -> g.city?.let { citiesInv[it] }
            else -> null
        }

    private fun checkBirthYear(year: Int): Int = when {
        year > 99 -> 99
        year < 0 -> 0
        else -> year
    }

    private fun filterByNull(nill: Boolean?, property: Any?) =
        if (nill != null) {
            when (nill) {
                true -> property != null
                false -> property == null
            }
        } else true


    override fun recommend(id: Int?, city: String?, country: String?, limit: Int): Sequence<InnerAccount> {
        return emptySequence()
    }

    private fun fullIdsSequence() = (maxId downTo 0).asSequence()
}
