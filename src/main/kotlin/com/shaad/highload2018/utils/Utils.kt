package com.shaad.highload2018.utils

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

fun <K> emptyMutableSet() : MutableSet<K> = mutableSet as MutableSet<K>
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
