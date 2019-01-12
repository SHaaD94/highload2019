package com.shaad.highload2018.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentHashMap

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

fun <K> concurrentHashSet(capacity: Int = 16): MutableSet<K> = ConcurrentHashMap.newKeySet(capacity)

fun <K> emptyMutableSet(): MutableSet<K> = mutableSet as MutableSet<K>
val mutableSet = mutableSetOf<Nothing>()

fun now() = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)

fun <T> customIntersects(
    sourceCollection: Collection<T>,
    vararg collections: Collection<T>
): Collection<T> {
    val notSourceCollection = collections.filter { it !== sourceCollection }
    if (notSourceCollection.isEmpty()) return sourceCollection
    return notSourceCollection.reduce { ac, collection -> ac.intersect(collection) }
}


fun convertToBytes(`object`: Any): ByteArray {
    ByteArrayOutputStream().use { bos ->
        ObjectOutputStream(bos).use { out ->
            out.writeObject(`object`)
            return bos.toByteArray()
        }
    }
}

inline fun <reified T> convertFromBytes(bytes: ByteArray): T =
    ByteArrayInputStream(bytes).use { bis -> ObjectInputStream(bis).use { `in` -> `in`.readObject() as T } }


fun <T> measureTimeAndReturnResult(opName: String = "", block: () -> T): T {
    val start = System.currentTimeMillis()
    val res = block()
    (System.currentTimeMillis() - start).let {
        if (it > 100) {
            println("$opName $it")
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

fun joinIterators(indexes: List<Iterator<Int>>) = iterator {
    if (indexes.isEmpty()) {
        return@iterator
    }
    val range = (0 until indexes.size)
    val currentVal = Array<Int?>(indexes.size) { null }
    range.forEach { i ->
        if (indexes[i].hasNext()) {
            currentVal[i] = indexes[i].next()
        }
    }
    while (true) {
        val maxValue = currentVal.asSequence().filter { it != null }.maxBy { it!! } ?: return@iterator
        yield(maxValue)
        range.forEach { it ->
            val c = currentVal[it] ?: return@forEach
            if (c == maxValue) {
                currentVal[it] = if (!indexes[it].hasNext()) {
                    null
                } else {
                    indexes[it].next()
                }
            }
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
    val range = (0 until indexes.size)
    val currentVal = Array<Int?>(indexes.size) { null }
    range.forEach { i ->
        if (!indexes[i].hasNext()) {
            return@sequence
        }
        currentVal[i] = indexes[i].next()
    }

    while (true) {
        var allEqual = true
        var id2Yield = -1
        (0 until indexes.size).forEach { i ->
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
        }
        if (allEqual) {
            yield(id2Yield)
            range.forEach { i ->
                if (!indexes[i].hasNext()) {
                    return@sequence
                }
                currentVal[i] = indexes[i].next()
            }
        } else {
            range.forEach { i ->
                if (currentVal[i]!! >= id2Yield) {
                    while (currentVal[i]!! > id2Yield) {
                        if (!indexes[i].hasNext()) {
                            return@sequence
                        }
                        currentVal[i] = indexes[i].next()
                    }

                }
            }
        }
    }
}


fun getIterator(array: Array<ArrayList<Int>>?): Iterator<Int> {
    array ?: return emptyIterator()
    val resultIterators = array.filter { !it.isEmpty() }.map { it.iterator() }
    return joinIterators(resultIterators)
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

        if (target < nums[mid]) {
            i = mid + 1
        } else if (target > nums[mid]) {
            j = mid - 1
        } else {
            return mid
        }
    }

    return i
}

