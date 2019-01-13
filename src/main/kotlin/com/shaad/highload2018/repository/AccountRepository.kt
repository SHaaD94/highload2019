package com.shaad.highload2018.repository

import com.google.common.primitives.UnsignedBytes
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.FilterRequest
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest

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
            if (domain != null) indexes.add(getIterator(emailDomainIndex[domain]))
        }
        filterRequest.sex?.let { (eq) ->
            indexes.add(getIterator(sexIndex[eq]))
        }
        filterRequest.premium?.let { (now, _) ->
            if (now != null) {
                indexes.add(getIterator(premiumNowIndex))
            }
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

                val ltIterator = getIterator(birthIndex[ltY])
                if (lt == null) ltIterator else {
                    ltIterator.asSequence().filter {
                        val acc = getAccountByIndex(it)!!
                        acc.birth <= lt
                    }.iterator()
                }.let { iterators.add(it) }


                val gtIterator = getIterator(birthIndex[gtY])
                if (gt == null) gtIterator else {
                    gtIterator.asSequence().filter {
                        val acc = getAccountByIndex(it)!!
                        acc.birth >= gt
                    }.iterator()
                }.let { iterators.add(it) }

                indexes.add(joinIterators(iterators))
            }

            if (year != null) indexes.add(getIterator(birthIndex[checkBirthYear(year - 1920)]))
        }

        filterRequest.interests?.let { (contains, any) ->
            contains?.let { containsInterests ->
                containsInterests.asSequence()
                    .map { getIterator(interests[it]?.let { interestIndex[it] }) }
                    .forEach { indexes.add(it) }
            }
            any?.let { anyInterests ->
                anyInterests.map { getIterator(interests[it]?.let { interestIndex[it] }) }
            }?.let { indexes.add(joinIterators(it)) }
        }

        filterRequest.likes?.let { (contains) ->
            contains?.let { likes ->
                likes.asSequence().map { getLikesByIndex(it).iterator() }.forEach { indexes.add(it) }
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

        val useSex = groupRequest.keys.contains("sex")
        val useStatus = groupRequest.keys.contains("status")
        val useInterest = groupRequest.keys.contains("interests")
        val useCountry = groupRequest.keys.contains("country")
        val useCity = groupRequest.keys.contains("city")

        val sequence = if (indexes.isEmpty()) fullIdsSequence() else generateSequenceFromIndexes(indexes)

        //sex->status->country->city->interests
        val map =
            mutableMapOf<Int?, MutableMap<Int?, MutableMap<Int?, MutableMap<Int?, MutableMap<Int?, Int?>>>>>()
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
                interests.forEach { interest -> cityMap.compute(interest) { _, value -> (value ?: 0) + 1 } }
            }

        val tempGroups = mutableListOf<Group>()
        map.entries.forEach { (sex, statusMap) ->
            statusMap.forEach { (status, countryMap) ->
                countryMap.forEach { (country, cityMap) ->
                    cityMap.forEach { city, interests ->
                        interests.forEach { interest, count ->
                            tempGroups.add(Group(sex, status, interest, country, city, count))
                        }
                    }
                }
            }
        }

        fun getComparator(pos: Int, g: Group): String? =
            when (groupRequest.keys.getOrNull(pos)) {
                "sex" -> int2Sex(g.sex).toString()
                "status" -> g.status?.let { statusesInv[it] }
                "interests" -> g.interest?.let { interestsInv[it] }
                "country" -> g.country?.let { countriesInv[it] }
                "city" -> g.city?.let { citiesInv[it] }
                else -> null
            }

        val comparator = compareBy<Group>({ it.count },
            { getComparator(0, it) }, { getComparator(1, it) }, { getComparator(2, it) },
            { getComparator(3, it) }, { getComparator(4, it) })

        return sequence {
            var iterations = groupRequest.limit
            val checkedEntries = mutableListOf<Any>()
            while (iterations != 0) {
                val element =
                    tempGroups
                        .asSequence()
                        .filter { e -> checkedEntries.none { e === it } }
                        .let { groups ->
                            if (groupRequest.order == 1) groups.minWith(comparator) else groups.maxWith(comparator)
                        } ?: return@sequence
                checkedEntries.add(element)
                iterations--
                yield(element)
            }
        }
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

    private fun fullIdsSequence() = sequence {
        var id = maxId
        while (id >= 0) {
            yield(id)
            id--
        }
    }
}


