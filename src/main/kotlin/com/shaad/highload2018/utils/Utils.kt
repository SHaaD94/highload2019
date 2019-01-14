package com.shaad.highload2018.utils

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*

val moscowTimeZone = ZoneOffset.ofHours(3)
fun parsePhoneCode(phone: String): String {
    var code = ""
    var scrapping = false
    for (i in (0 until phone.length)) {
        if (phone[i] == '(') {
            scrapping = true
            continue
        }
        if (phone[i] == ')') {
            return code
        }
        if (scrapping) {
            code += phone[i]
        }
    }
    throw RuntimeException("Failed to parse code of phone $phone")
}


fun <T> measureTimeAndReturnResult(opName: String = "", block: () -> T): T {
    val start = System.currentTimeMillis()
    val res = block()
    (System.currentTimeMillis() - start).let {
        if (it > 1000) {
            println("$opName $it")
        }
    }
    return res
}

fun <T> measureTimeAndReturnResultLazy(opName: () -> String = { "" }, block: () -> T): T {
    val start = System.currentTimeMillis()
    val res = block()
    (System.currentTimeMillis() - start).let {
        if (it > 100) {
            println("${opName()} $it")
        }

    }
    return res
}

class CompositeSet(val sets: Collection<Set<Int>>) : Set<Int> {
    override val size = sets.sumBy { it.size }
    override fun containsAll(elements: Collection<Int>): Boolean {
        TODO("not implemented")
    }

    override fun isEmpty(): Boolean = sets.all { it.isEmpty() }

    override fun iterator(): Iterator<Int> {
        TODO("not implemented")
    }

    override fun spliterator(): Spliterator<Int> {
        TODO("not implemented")
    }

    override fun contains(element: Int): Boolean = sets.any { it.contains(element) }
}

object EmptyIterator : Iterator<Nothing> {
    override fun hasNext() = false

    override fun next(): Nothing {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

fun emptyIterator() = EmptyIterator

fun joinIterators(indexes: List<Iterator<Int>>) = iterator<Int> {
    if (indexes.isEmpty()) {
        return@iterator
    }
    val currentVal = Array<Int?>(indexes.size) { null }
    var i = 0
    while (i < indexes.size) {
        if (indexes[i].hasNext()) {
            currentVal[i] = indexes[i].next()
        }
        i++
    }
    while (true) {
        var maxValue: Int? = null
        var j = 0
        while (j < currentVal.size) {
            if (currentVal[j] == null) {
                j++
                continue
            }
            val it = currentVal[j]
            if (maxValue == null) {
                maxValue = it
            }
            if (maxValue!! < it!!) {
                maxValue = it
            }
            j++
        }
        if (maxValue == null) {
            return@iterator
        }
        yield(maxValue)
        i = 0
        while (i < indexes.size) {
            if (currentVal[i] == null) {
                i++
                continue
            }
            val c = currentVal[i]
            if (c == maxValue) {
                currentVal[i] = if (!indexes[i].hasNext()) {
                    null
                } else {
                    indexes[i].next()
                }
            }
            i++
        }
    }
}

fun getYear(timestamp: Int): Int {
    return LocalDateTime.ofEpochSecond(timestamp.toLong(), 0, moscowTimeZone).year
}

fun generateSequenceFromIndexes(indexes: List<Iterator<Int>>): Sequence<Int> = sequence {
    if (indexes.isEmpty()) {
        return@sequence
    }
    var i = 0
    val currentVal = Array<Int?>(indexes.size) { null }
    while (i < indexes.size) {
        if (!indexes[i].hasNext()) {
            return@sequence
        }
        currentVal[i] = indexes[i].next()
        i++
    }

    while (true) {
        var allEqual = true
        var id2Yield = -1
        i = 0
        while (i < indexes.size) {
            val c = currentVal[i]!!
            if (id2Yield == -1) {
                id2Yield = c
            } else {
                if (id2Yield != c) {
                    allEqual = false
                }
                if (id2Yield > c) {
                    id2Yield = c
                }
            }
            i++
        }
        if (allEqual) {
            yield(id2Yield)
            i = 0
            while (i < indexes.size) {
                if (!indexes[i].hasNext()) {
                    return@sequence
                }
                currentVal[i] = indexes[i].next()
                i++
            }
        } else {
            i = 0
            while (i < indexes.size) {
                if (currentVal[i]!! >= id2Yield) {
                    while (currentVal[i]!! > id2Yield) {
                        if (!indexes[i].hasNext()) {
                            return@sequence
                        }
                        currentVal[i] = indexes[i].next()
                    }
                }
                i++
            }
        }
    }
}

fun getPartitionedIterator(array: Array<ArrayList<Int>>?): Iterator<Int> {
    array ?: return emptyIterator()
    return object : Iterator<Int> {
        private var currIterator: Iterator<Int>? = null
        private var curr = 0

        init {
            if (!array.isEmpty()) {
                currIterator = array[curr].iterator()
            }
        }

        override fun hasNext() = currIterator?.hasNext() ?: false

        override fun next(): Int {
            check(currIterator != null)
            if (currIterator!!.hasNext()) {
                val e = currIterator!!.next()
                return e
            } else {
                curr++
                currIterator = if (curr < array.size) array[curr].iterator() else null
            }
            throw RuntimeException("Should not be here")
        }
    }
}

fun addToSortedCollection(list: ArrayList<Int>, id: Int?) {
    synchronized(list) {
        val closest = searchClosest(id!!, list)

        list.add(closest, id)
    }
}

fun searchClosest(target: Int, nums: ArrayList<Int>): Int {
    var i = 0
    var j = nums.size - 1

    while (i <= j) {
        val mid = (i + j) / 2

        when {
            target < nums[mid] -> i = mid + 1
            target > nums[mid] -> j = mid - 1
            else -> return mid
        }
    }

    return i
}

