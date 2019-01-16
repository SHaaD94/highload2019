package com.shaad.highload2018.utils

import org.agrona.collections.IntArrayList
import java.time.LocalDateTime
import java.time.ZoneOffset

val moscowTimeZone = ZoneOffset.ofHours(3)
fun parsePhoneCode(phone: String): String {
    var code = ""
    var scrapping = false
    var i = 0
    while (i < phone.length) {
        if (phone[i] == '(') {
            scrapping = true
            i++
            continue
        }
        if (phone[i] == ')') {
            return code
        }
        if (scrapping) {
            code += phone[i]
        }
        i++
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

object EmptyIterator : Iterator<Nothing> {
    override fun hasNext() = false
    override fun next(): Nothing {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

object EmptyIntIterator : IntIterator() {
    override fun hasNext() = false
    override fun nextInt(): Int {
        TODO("not implemented")
    }
}

fun emptyIterator() = EmptyIterator
fun emptyIntIterator() = EmptyIntIterator

//todo IntIterator
fun joinIterators(indexes: List<Iterator<Int>>) = iterator {
    if (indexes.isEmpty()) {
        return@iterator
    }
    val currentVal = IntArray(indexes.size) { IntArrayList.DEFAULT_NULL_VALUE }
    var i = 0
    while (i < indexes.size) {
        if (indexes[i].hasNext()) {
            currentVal[i] = indexes[i].next()
        }
        i++
    }
    while (true) {
        var maxValue: Int = IntArrayList.DEFAULT_NULL_VALUE
        var j = 0
        while (j < currentVal.size) {
            if (currentVal[j] == IntArrayList.DEFAULT_NULL_VALUE) {
                j++
                continue
            }
            val it = currentVal[j]
            if (maxValue == IntArrayList.DEFAULT_NULL_VALUE) {
                maxValue = it
            }
            if (maxValue < it) {
                maxValue = it
            }
            j++
        }
        if (maxValue == IntArrayList.DEFAULT_NULL_VALUE) {
            return@iterator
        }
        yield(maxValue)
        i = 0
        while (i < indexes.size) {
            if (currentVal[i] == IntArrayList.DEFAULT_NULL_VALUE) {
                i++
                continue
            }
            val c = currentVal[i]
            if (c == maxValue) {
                currentVal[i] = if (!indexes[i].hasNext()) {
                    IntArrayList.DEFAULT_NULL_VALUE
                } else {
                    indexes[i].next()
                }
            }
            i++
        }
    }
}.iterator()

fun getYear(timestamp: Int): Int {
    return LocalDateTime.ofEpochSecond(timestamp.toLong(), 0, moscowTimeZone).year
}

fun generateSequenceFromIndexes(indexes: List<Iterator<Int>>): Sequence<Int> =
    if (indexes.isEmpty()) emptySequence() else
        sequence {
            if (indexes.isEmpty()) {
                return@sequence
            }
            var i = 0
            val currentVal = IntArray(indexes.size) { IntArrayList.DEFAULT_NULL_VALUE }
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
                    val c = currentVal[i]
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
                        if (currentVal[i] >= id2Yield) {
                            while (currentVal[i] > id2Yield) {
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

fun Array<IntArrayList>?.getPartitionedIterator(): IntIterator {
    this ?: return emptyIntIterator()
    return object : IntIterator() {
        private val lists = this@getPartitionedIterator.filter { !it.isEmpty() }
        private var curNum = 0
        private var hasNext = !lists.isEmpty()
        private var curList = lists.size - 1

        override fun hasNext() = hasNext

        override fun nextInt(): Int {
            return if (curNum <= lists[curList].size - 1) {
                val number = lists[curList][curNum++]
                hasNext = curNum <= lists[curList].size - 1 || curList > 0
                number
            } else {
                curNum = 0
                val number = lists[--curList][curNum]
                hasNext = curNum <= lists[curList].size - 1 || curList > 0
                number
            }
        }
    }
}

fun addToSortedCollection(list: IntArrayList, id: Int) {
    synchronized(list) {
        val closest = searchClosest(id, list)

        list.addInt(closest, id)
    }
}

fun searchClosest(target: Int, nums: MutableList<Int>): Int {
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

