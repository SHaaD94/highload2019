package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.generateIteratorFromIndexes
import com.shaad.highload2018.utils.getPartitionedIterator
import com.shaad.highload2018.utils.intIterator
import com.shaad.highload2018.utils.joinIterators
import kotlin.math.abs

fun suggest(id: Int, city: String?, country: String?, limit: Int): Sequence<InnerAccount> {
    val specifiedAcc = getAccountByIndex(id) ?: throw RuntimeException("User $id not found")

    val sameSex = sexIndex[specifiedAcc.sex]

    val countryIndex = country?.let { countries[it] }?.let { countryIndex[it] }
    val cityIndex = city?.let { cities[it] }?.let { cityIndex[it] }

    if ((country != null && countryIndex == null) || (city != null && cityIndex == null)) {
        return emptySequence()
    }

    if (specifiedAcc.likes == null) {
        return emptySequence()
    }

    val likes = joinIterators(specifiedAcc.likes!!.map { getLikesByIndex(it)!!.intIterator() })

    val indexes = ArrayList<IntIterator>(4)
    indexes.add(likes)
    indexes.add(sameSex.getPartitionedIterator())
    if (countryIndex != null) {
        indexes.add(countryIndex.getPartitionedIterator())
    }
    if (cityIndex != null) {
        indexes.add(cityIndex.getPartitionedIterator())
    }

    var maxSimilarUser: InnerAccount? = null
    var maxSim = 0.0
    val iterator = generateIteratorFromIndexes(indexes)
    while (iterator.hasNext()) {
        val acc = getAccountByIndex(iterator.nextInt())!!
        if (acc === specifiedAcc) {
            continue
        }

        var points = 0.0
        var counter = 0.0
        //todo: optimization point - sort likes by id desc?
        for (i in 0 until acc.likes!!.size) {
            for (j in 0 until specifiedAcc.likes!!.size) {
                if (specifiedAcc.likes!!.getInt(j) == acc.likes!!.getInt(i)) {
                    points += abs(specifiedAcc.likeTs!!.getInt(j) - acc.likeTs!!.getInt(i))
                    counter += 1.0
                }
            }
        }
        val sim = if (points == 0.0) 1.0 else 1.0 / (points / counter)
        if (sim > maxSim) {
            maxSim = sim
            maxSimilarUser = acc
        }
    }

    var cId = maxId + 1
    val users = ArrayList<InnerAccount>(limit)
    while (cId > 0 && users.size < limit) {
        cId--
        val a = getAccountByIndex(cId) ?: continue
        if (!specifiedAcc.likes!!.containsInt(a.id) && maxSimilarUser!!.likes!!.containsInt(a.id)) {
            users.add(a)
        }
    }
    return users.asSequence()
}
