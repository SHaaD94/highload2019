package com.shaad.highload2018.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentHashMap


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

class CompositeSet(private val sets: Collection<Set<Int>>) : Set<Int> {
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

fun generateSequenceFromIndexes(indexes: MutableList<List<Int>>): Sequence<Int> {
    return sequence {
        val counters = Array(indexes.size) { 0 }
        val currentVal = Array<Int?>(indexes.size) { null }
        while (true) {
            (0 until indexes.size).forEach { i ->
                currentVal[i] = indexes[i].getOrNull(counters[i])
            }
            var allEqual = true
            var idToYield = -1
            (0 until indexes.size).forEach { i ->
                val c = currentVal[i] ?: return@sequence
                if (idToYield == -1) {
                    idToYield = c
                } else {
                    allEqual = false
                    if (idToYield < c) {
                        idToYield = c
                    }
                }
            }
            if (allEqual) {
                yield(idToYield)
                counters.forEachIndexed{ i ,_ -> counters[i]++ }
            } else {
                counters.forEachIndexed{ i ,_ ->
                    if (currentVal[i]!! >= idToYield) {
                        counters[i]++
                    }
                }
            }
        }
    }
}

