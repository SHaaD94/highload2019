package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.addToSortedCollection
import com.shaad.highload2018.utils.getYear
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.utils.parsePhoneCode
import org.agrona.collections.IntArrayList


fun addAccount(account: Account) {
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
            },
            account.likes?.let {
                val intArray = IntArrayList()
                for (i in 0 until it.size){
                    intArray.addInt(it[i].id)
                    intArray.addInt(it[i].ts)
                }
                intArray
            }
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
                likeIdInt < 250_000 -> likeIndex0_250[likeIdInt]
                likeIdInt in 250_000 until 500_000 -> likeIndex250_500[likeIdInt - 250_000]
                likeIdInt in 500_000 until 750_000 -> likeIndex500_750[likeIdInt - 500_000]
                likeIdInt in 750_000 until 1_000_000 -> likeIndex750_1000[likeIdInt - 750_000]
                likeIdInt in 1_000_000 until 1_300_000 -> likeIndex1000_1300[likeIdInt - 1_000_000]
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
