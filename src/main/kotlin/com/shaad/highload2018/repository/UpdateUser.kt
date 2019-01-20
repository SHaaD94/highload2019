package com.shaad.highload2018.repository

import com.shaad.highload2018.utils.addToSortedCollection
import com.shaad.highload2018.utils.getYear
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.utils.parsePhoneCode
import com.wizzardo.tools.json.JsonArray
import com.wizzardo.tools.json.JsonObject
import org.agrona.collections.IntArrayList

fun updateUser(
    id: Int,
    newEmail: String?,
    newFname: Int?,
    newSname: Int?,
    newPhone: String,
    newSex: Int?,
    newBirth: Int?,
    newJoined: Int?,
    newCity: Int?,
    newCountry: Int?,
    newStatus: Int?,
    newInterests: JsonArray?,
    newPremium: JsonObject?,
    newLikes: JsonArray?
) {

    val account = getAccountByIndex(id)
    check(account != null) { "User $id does not exist" }

    if (newSex != null && newSex != account.sex) {
        val bucket = getIdBucket(account.id, sexIndex[account.sex])
        synchronized(bucket) {
            bucket.removeInt(account.id)
        }
        val newBucket = getIdBucket(account.id, sexIndex[newSex])
        synchronized(newBucket) {
            addToSortedCollection(newBucket, account.id)
        }
    }
    if (newFname != null && newFname != account.sex) {
        if (account.fname!=null) {
            val bucket = getIdBucket(account.id, fnameIndex[account.fname])
            synchronized(bucket) {
                bucket.removeInt(account.id)
            }
        }
        val newBucket = getIdBucket(account.id, fnameIndex[newFname])
        synchronized(newBucket) {
            addToSortedCollection(newBucket, account.id)
        }
    }
//
//        val innerAccount = InnerAccount(
//            account.id,
//            writeNormalizationIndex(statuses, statusesInv, statusesIdCounter, account.status),
//            account.email.toByteArray(),
//            if (account.sex == 'm') 0 else 1,
//            account.fname?.let { writeNormalizationIndex(fnames, fnamesInv, fnamesIdCounter, it) },
//            account.sname?.let { writeNormalizationIndex(snames, snamesInv, snamesIdCounter, it) },
//            account.city?.let { writeNormalizationIndex(cities, citiesInv, citiesIdCounter, it) },
//            account.country?.let { writeNormalizationIndex(countries, countriesInv, countriesIdCounter, it) },
//            account.birth,
//            account.phone?.toByteArray(),
//            account.premium,
//            account.interests?.map {
//                writeNormalizationIndex(
//                    interests,
//                    interestsInv,
//                    interestsIdCounter,
//                    it
//                )
//            }?.let { intArrayList ->
//                val intArray = IntArrayList()
//                intArrayList.forEach { intArray.addInt(it) }
//                intArray
//            },
//            account.likes?.let {
//                val intArray = IntArrayList(it.size, IntArrayList.DEFAULT_NULL_VALUE)
//                val intTsArray = IntArrayList(it.size, IntArrayList.DEFAULT_NULL_VALUE)
//                for (i in 0 until it.size) {
//                    intArray.addInt(it[i].id)
//                    intTsArray.addInt(it[i].ts)
//                }
//                intArray
//            },
//            account.likes?.let {
//                val intTsArray = IntArrayList(it.size, IntArrayList.DEFAULT_NULL_VALUE)
//                for (i in 0 until it.size) {
//                    intTsArray.addInt(it[i].ts)
//                }
//                intTsArray
//            }
//        )
//        when {
//            account.id < 1_300_000 -> accountsIndex[(account.id / 50_000)][account.id % 50_000] = innerAccount
//            else -> accounts1300[account.id] = innerAccount
//        }
//    }

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
//            measureTimeAndReturnResult("email index:") {
//                addEmailToIndex(email, account.id)
//            }
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
            var collection = when {
                likeIdInt < 1_300_000 -> likeIndex[(likeIdInt / 50_000)][likeIdInt % 50_000]
                else -> null
            }
            if (collection == null) {
                collection = likeIndex1300.computeIfAbsent(likeIdInt) { IntArrayList() }
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