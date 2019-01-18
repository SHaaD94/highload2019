package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*

private const val free = "свободны"
private const val complicated = "всё сложно"
private const val occupied = "заняты"
fun recommend(id: Int, city: String?, country: String?, limit: Int): Sequence<InnerAccount> {
    val specifiedAcc = getAccountByIndex(id) ?: throw RuntimeException("User $id not found")

    val oppositeSex = sexIndex[if (specifiedAcc.sex == 0) 1 else 0]

    val countryIndex = country?.let { countries[it] }?.let { countryIndex[it] }
    val cityIndex = city?.let { cities[it] }?.let { cityIndex[it] }

    if ((country != null && countryIndex == null) || (city != null && cityIndex == null)) {
        return emptySequence()
    }

    val interestIndexes = specifiedAcc.interests?.map { interestIndex[it] } ?: return emptySequence()

    fun getUsersByStatus(status: String, usePremium: Boolean): Iterator<InnerAccount> {
        val statusIterator = statusIndex[statuses[status]!!].getPartitionedIterator()

        val interestIterator = joinIterators(interestIndexes.map { it.getPartitionedIterator() })

        val iterators = ArrayList<IntIterator>(6)
        if (countryIndex != null) iterators.add(countryIndex.getPartitionedIterator())
        if (cityIndex != null) iterators.add(cityIndex.getPartitionedIterator())
        if (usePremium) iterators.add(premiumNowIndex.getPartitionedIterator())
        iterators.add(interestIterator)
        iterators.add(oppositeSex.getPartitionedIterator())
        iterators.add(statusIterator)

        val resultIterator = generateIteratorFromIndexes(iterators)

        val usersByInterests = Array<ArrayList<InnerAccount>?>(specifiedAcc.interests.size) { null }
        while (resultIterator.hasNext()) {
            val acc = getAccountByIndex(resultIterator.nextInt())!!
            if (acc === specifiedAcc) {
                continue
            }

            var commonInterests = -1
            for (i in 0 until acc.interests!!.size) {
                if (specifiedAcc.interests.containsInt(acc.interests.getInt(i))) {
                    commonInterests++
                }
            }
            var bucket = usersByInterests[commonInterests]
            if (bucket == null) {
                bucket = ArrayList()
                usersByInterests[commonInterests] = bucket
            }
            bucket.add(searchClosestAccountByAction(acc, bucket) { Math.abs(specifiedAcc.birth - it.birth) }, acc)
        }
        return object : Iterator<InnerAccount> {
            private val premiumIterator = if (usePremium) emptyIntIterator() else premiumNowIndex.getPartitionedIterator()
            private var premCur = Int.MAX_VALUE
            private var hasNext = true
            private var next: InnerAccount? = null
            private var curList = usersByInterests.size
            private var curPos = -1

            init {
                findNext()
            }

            override fun hasNext() = hasNext

            override fun next(): InnerAccount {
                val next = this.next
                findNext()
                return next!!
            }

            private fun findNext() {
                while (true) {
                    if (curPos == -1) {
                        curList--

                        while (curList > 0 && usersByInterests[curList] == null) {
                            curList--
                            curPos = usersByInterests[curList]?.let { it.size - 1 } ?: -1
                        }
                        if (curList < 0 || usersByInterests[curList] == null) {
                            hasNext = false
                            return
                        }
                        curPos = usersByInterests[curList]!!.size - 1
                    }
                    next = usersByInterests[curList]!![curPos]
                    curPos--
                    while (premCur > next!!.id && premiumIterator.hasNext()) {
                        premCur = premiumIterator.nextInt()
                    }
                    if (premCur!=next!!.id){
                        break
                    }
                }
            }
        }
    }

    val result = ArrayList<InnerAccount>(limit)
    fun readIterator(accounts: Iterator<InnerAccount>): Boolean {
        while (accounts.hasNext() && result.size < limit) {
            result.add(accounts.next())
        }
        return accounts.hasNext()
    }

    if (readIterator(getUsersByStatus(free, true))) return result.asSequence()
    if (readIterator(getUsersByStatus(complicated, true))) return result.asSequence()
    if (readIterator(getUsersByStatus(occupied, true))) return result.asSequence()
    if (readIterator(getUsersByStatus(free, false))) return result.asSequence()
    if (readIterator(getUsersByStatus(complicated, false))) return result.asSequence()
    if (readIterator(getUsersByStatus(occupied, false))) return result.asSequence()

    return result.asSequence()
}

private fun <T> searchClosestAccountByAction(
    target: T,
    accounts: ArrayList<T>,
    action: (T) -> Int
): Int {
    var i = 0
    var j = accounts.size - 1

    while (i <= j) {
        val mid = (i + j) / 2

        when {
            action(target) < action(accounts[mid]) -> i = mid + 1
            action(target) > action(accounts[mid]) -> j = mid - 1
            else -> return mid
        }
    }

    return i
}
