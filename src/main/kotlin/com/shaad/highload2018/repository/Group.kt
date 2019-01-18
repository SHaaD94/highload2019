package com.shaad.highload2018.repository

import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.Group
import com.shaad.highload2018.web.get.GroupRequest

private val listWithZero = listOf(0)
fun group(groupRequest: GroupRequest): Sequence<Group> {
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
}


fun getGroupComparingString(groupRequest: GroupRequest, pos: Int, g: Group): String? =
    when (groupRequest.keys.getOrNull(pos)) {
        "sex" -> g.sex?.let { int2Sex(it) }?.let {
            when (it) {
                'm' -> "m"
                'f' -> "f"
                else -> throw RuntimeException("wrong sex $it")
            }
        }
        "status" -> g.status?.let { statusesInv[it] }
        "interests" -> g.interest?.let { interestsInv[it] }
        "country" -> g.country?.let { countriesInv[it] }
        "city" -> g.city?.let { citiesInv[it] }
        else -> null
    }
