package com.shaad.highload2018.repository

import com.google.common.primitives.UnsignedBytes
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.FilterRequest
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest
import org.agrona.collections.IntArrayList

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): Sequence<InnerAccount>
    fun group(groupRequest: GroupRequest): Sequence<Group>
    fun recommend(id: Int, city: String?, country: String?, limit: Int): Sequence<InnerAccount>
}

class AccountRepositoryImpl : AccountRepository {
    @Volatile
    private var maxId = 0

    override fun addAccount(account: Account) {
        check(getAccountByIndex(account.id) == null) { "User ${account.id} already exists" }

        measureTimeAndReturnResult("id index") {
            val innerAccount = InnerAccount(
                account.id,
                writeNormalizationIndex(statuses, statusesInv, statusesIdCounter, account.status),
                account.email.toByteArray(),
                if (account.sex == 'm') 0 else 1,
                account.fname?.let { writeNormalizationIndex(fnames, fnamesInv, fnamesIdCounter, it) },
                account.sname?.let { writeNormalizationIndex(snames, snamesInv, snamesIdCounter, it) },
                account.city?.let { writeNormalizationIndex(cities, citiesInv, citiesIdCounter, it) },
                account.country?.let { writeNormalizationIndex(countries, countriesInv, countriesIdCounter, it) },
                account.birth,
                account.phone?.toByteArray(),
                account.premium,
                account.interests?.map {
                    writeNormalizationIndex(
                        interests,
                        interestsInv,
                        interestsIdCounter,
                        it
                    )
                }?.let { intArrayList ->
                    val intArray = IntArrayList()
                    intArrayList.forEach { intArray.addInt(it) }
                    intArray
                }
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
            val collection = sexIndex[sex2Int(account.sex)]
            addToSortedCollection(getIdBucket(account.id, collection), account.id)
        }

        measureTimeAndReturnResult("status index:") {
            val collection = statusIndex[statuses[account.status]!!]
            addToSortedCollection(getIdBucket(account.id, collection), account.id)
        }

        measureTimeAndReturnResult("fname index:") {
            account.fname?.let {
                val collection = fnameIndex[fnames[it]!!]
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }
        }

        measureTimeAndReturnResult("sname index:") {
            account.sname?.let {
                val collection = snameIndex[snames[it]!!]
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }
        }

        account.email.let { email ->
            measureTimeAndReturnResult("email domain index:") {
                val domain = email.split("@").let { parsedEmail -> parsedEmail[parsedEmail.size - 1] }
                val collection = emailDomainIndex.computeIfAbsent(domain) { Array(20) { IntArrayList() } }
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
                val collection = countryIndex[countries[it]!!]
                addToSortedCollection(getIdBucket(account.id, collection), account.id)
            }
        }

        measureTimeAndReturnResult("city index:") {
            account.city?.let {
                val collection = cityIndex[cities[it]!!]
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
                val collection = interestIndex[interests[it]!!]
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
                val likeId = likeIdInt
                var collection = when {
                    likeId < 250_000 -> likeIndex0_250[likeId]
                    likeId in 250_000 until 500_000 -> likeIndex250_500[likeId - 250_000]
                    likeId in 500_000 until 750_000 -> likeIndex500_750[likeId - 500_000]
                    likeId in 750_000 until 1_000_000 -> likeIndex750_1000[likeId - 750_000]
                    likeId in 1_000_000 until 1_300_000 -> likeIndex1000_1300[likeId - 1_000_000]
                    else -> null
                }
                if (collection == null) {
                    collection = likeIndex1300.computeIfAbsent(likeId) { IntArrayList() }
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
        val indexes = mutableListOf<IntIterator>()
        filterRequest.email?.let { (domain, _, _) ->
            if (domain != null) indexes.add(emailDomainIndex[domain].getPartitionedIterator())
        }
        filterRequest.sex?.let { (eq) ->
            indexes.add(sexIndex[sex2Int(eq)].getPartitionedIterator())
        }
        filterRequest.premium?.let { (now, _) ->
            if (now != null) {
                indexes.add(premiumNowIndex.getPartitionedIterator())
            }
        }
        filterRequest.status?.let { (eq, neq) ->
            if (eq != null) {
                indexes.add(statusIndex[statuses[eq]!!].getPartitionedIterator())
            }
            if (neq != null) {
                statuses.keys().asSequence().filter { it != neq }.map { statuses[it]!! }
                    .map { statusIndex[it].getPartitionedIterator() }
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


                val iterators = (gtY..ltY).asSequence()
                    .map { birthIndex[it] }
                    .map { it.getPartitionedIterator() }
                    .toMutableList()

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
                likes.asSequence().map { getLikesByIndex(it)?.intIterator() ?: emptyIntIterator() }
                    .forEach { indexes.add(it) }
            }
        }

        val iterator = if (indexes.isEmpty()) fullIdsIterator() else generateIteratorFromIndexes(indexes)

        val result = ArrayList<InnerAccount>(filterRequest.limit)
        while (iterator.hasNext() && result.size != filterRequest.limit) {
            val acc = getAccountByIndex(iterator.nextInt()) ?: continue

            val isFilteredByEmail = if (filterRequest.email != null) {
                val ltFilter = if (filterRequest.email.lt != null) {
                    lexComparator.compare(acc.email, filterRequest.email.lt) <= 0
                } else true

                val gtFilter = if (filterRequest.email.gt != null) {
                    lexComparator.compare(acc.email, filterRequest.email.gt) >= 0
                    true
                } else true
                ltFilter && gtFilter
            } else true

            if (!isFilteredByEmail) {
                continue
            }

            val isFilteredByNull = filterByNull(filterRequest.fname?.nill, acc.fname) &&
                    filterByNull(filterRequest.sname?.nill, acc.sname) &&
                    filterByNull(filterRequest.phone?.nill, acc.phone) &&
                    filterByNull(filterRequest.country?.nill, acc.country) &&
                    filterByNull(filterRequest.city?.nill, acc.city) &&
                    filterByNull(filterRequest.premium?.nill, acc.premium)

            if (!isFilteredByNull) {
                continue
            }

            val isFilteredByAge = if (filterRequest.birth != null) {
                val ltFilter = if (filterRequest.birth.lt != null) {
                    acc.birth <= filterRequest.birth.lt
                } else true

                val gtFilter = if (filterRequest.birth.gt != null) {
                    acc.birth >= filterRequest.birth.gt
                    true
                } else true
                ltFilter && gtFilter
            } else true

            if (!isFilteredByAge) {
                continue
            }
            result.add(acc)
        }
        return result.asSequence()
    }

    private val listWithZero = listOf(0)
    override fun group(groupRequest: GroupRequest): Sequence<Group> {
        val indexes = mutableListOf<IntIterator>()

        groupRequest.sname?.let { snames[it] }?.let { snameIndex[it] }?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.fname?.let { fnames[it] }?.let { fnameIndex[it] }?.let { indexes.add(it.getPartitionedIterator()) }
        groupRequest.sex?.let { sexIndex[sex2Int(it)] }?.let { indexes.add(it.getPartitionedIterator()) }

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
        groupRequest.likes?.let { getLikesByIndex(it)?.intIterator() ?: emptyIntIterator() }
            ?.let { indexes.add(it) }

        val useSex = groupRequest.keys.contains("sex")
        val useStatus = groupRequest.keys.contains("status")
        val useInterest = groupRequest.keys.contains("interests")
        val useCountry = groupRequest.keys.contains("country")
        val useCity = groupRequest.keys.contains("city")

        val iterator = if (indexes.isEmpty()) fullIdsIterator() else generateIteratorFromIndexes(indexes)

        //sex->status->country->city->interests
        val arrays = Array(if (useSex) 2 else 1) {
            Array(if (useStatus) 4 else 1) {
                Array(if (useCountry) countriesIdCounter.get() + 1 else 1) {
                    Array(if (useCity) citiesIdCounter.get() + 1 else 1) {
                        IntArray(if (useInterest) interestsIdCounter.get() + 1 else 1) {
                            0
                        }
                    }
                }
            }
        }
        // 0 is null
        while (iterator.hasNext()) {
            val acc = getAccountByIndex(iterator.nextInt()) ?: continue

            val sexArray = arrays[if (useSex) acc.sex else 0]
            val statusArray = sexArray[if (useStatus) acc.status else 0]
            val countryArray = statusArray[if (useCountry) acc.country ?: 0 else 0]
            val cityMap = countryArray[if (useCity) acc.city ?: 0 else 0]
            val interests = if (useInterest) (acc.interests) ?: listWithZero else listWithZero

            var i = 0
            while (i < interests.size) {
                cityMap[interests[i]]++
                i++
            }
        }

        val tempGroups = ArrayList<Group>()
        var sexIt = 0
        while (sexIt < arrays.size) {
            var statusIt = 0
            val sexBucket = arrays[sexIt]
            while (statusIt < sexBucket.size) {
                var countryIt = 0
                val statusBucket = sexBucket[statusIt]
                while (countryIt < statusBucket.size) {
                    var cityIt = 0
                    val cityBucket = statusBucket[countryIt]
                    while (cityIt < cityBucket.size) {
                        var interestIt = 0
                        val interestBucket = cityBucket[cityIt]
                        while (interestIt < interestBucket.size) {
                            if (interestBucket[interestIt] != 0) {
                                tempGroups.add(
                                    Group(
                                        if (useSex) sexIt else null,
                                        if (statusIt != 0) statusIt else null,
                                        if (interestIt != 0) interestIt else null,
                                        if (countryIt != 0) countryIt else null,
                                        if (cityIt != 0) cityIt else null,
                                        interestBucket[interestIt]
                                    )
                                )
                            }
                            interestIt++
                        }
                        cityIt++
                    }
                    countryIt++
                }
                statusIt++
            }
            sexIt++
        }

        val comparator = compareBy<Group>({ it.count },
            { getGroupComparingString(groupRequest, 0, it) },
            { getGroupComparingString(groupRequest, 1, it) },
            { getGroupComparingString(groupRequest, 2, it) },
            { getGroupComparingString(groupRequest, 3, it) },
            { getGroupComparingString(groupRequest, 4, it) })

        var iterations = groupRequest.limit
        val result = LinkedHashSet<Group>()
        while (iterations != 0) {
            val element =
                tempGroups
                    .asSequence()
                    .filter { e -> !result.contains(e) }
                    .let { groups ->
                        if (groupRequest.order == 1) groups.minWith(comparator) else groups.maxWith(comparator)
                    } ?: break
            result.add(element)
            iterations--
        }
        return result.asSequence()
//        val result = LinkedHashSet<Group>()
//        var i = 0
//        while (i < groupRequest.limit && i < tempGroups.size) {
//            var resGroup: Group? = null
//            var j = 0
//            elemLoop@ while (j < tempGroups.size) {
//                val c = tempGroups[j]
//                if (result.contains(c)) {
//                    j++
//                    continue
//                }
//
//                if (resGroup == null) {
//                    resGroup = c
//                    j++
//                    continue
//                }
//
//                if (resGroup !== c) {
//                    val countComparison = resGroup.count - c.count
//                    if (countComparison < 0 && groupRequest.order < 0) {
//                        resGroup = c
//                    } else if (countComparison > 0 && groupRequest.order > 0) {
//                        resGroup = c
//                    } else if (countComparison == 0) {
//                        var propertyCounter = 0
//                        propertyLoop@ while (propertyCounter < 5) {
//                            val resString = getGroupComparingString(groupRequest, countComparison, resGroup!!)
//                            val cString = getGroupComparingString(groupRequest, countComparison, c)
//                            if (resString == null && cString != null) {
//                                break@propertyLoop
//                            }
//                            if (cString == null && resString != null) {
//                                resGroup = c
//                                break@propertyLoop
//                            }
//                            if (cString == resString) {
//                                propertyCounter++
//                                continue@propertyLoop
//                            }
//                            if (cString!! > resString!! && groupRequest.order < 0) {
//                                resGroup = c
//                                break@propertyLoop
//                            }
//                            if (cString < resString && groupRequest.order > 0) {
//                                resGroup = c
//                                break@propertyLoop
//                            }
//                            break@propertyLoop
//                        }
//                    }
//                }
//                j++
//            }
//            result.add(resGroup!!)
//            i++
//        }
//
//        return result.asSequence()
    }

    private fun getGroupComparingString(groupRequest: GroupRequest, pos: Int, g: Group): String? =
        when (groupRequest.keys.getOrNull(pos)) {
            "sex" -> g.sex?.let { int2Sex(it) }.toString()
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


    private val free = "свободны"
    private val complicated = "все сложно"
    private val occupied = "заняты"
    override fun recommend(id: Int, city: String?, country: String?, limit: Int): Sequence<InnerAccount> {
        val id = getAccountByIndex(id) ?: throw RuntimeException("User $id not found")

        val otherSex = sexIndex[if (id.sex == 0) 1 else 0]
        val premiumIndex = premiumNowIndex

        val countryIndex = countries[country]?.let { countryIndex[it] }
        val cityIndex = cities[city]?.let { cityIndex[it] }

        if (countryIndex == null || cityIndex == null) {
            return emptySequence()
        }




        return emptySequence()
    }

    private fun fullIdsIterator() = object : IntIterator() {
        private var cur = maxId
        override fun nextInt() = cur--
        override fun hasNext() = cur > 0
    }
}
