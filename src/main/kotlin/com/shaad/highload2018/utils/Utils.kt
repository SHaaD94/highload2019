package com.shaad.highload2018.utils

import java.time.LocalDateTime
import java.time.ZoneOffset
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

fun now() = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)

fun <T> customIntersects(
    sourceCollection: Collection<T>,
    vararg collections: Collection<T>
): Collection<T> {
    val notSourceCollection = collections.filter { it !== sourceCollection }
    if (notSourceCollection.isEmpty()) return sourceCollection
    return notSourceCollection.reduce { ac, collection -> ac.intersect(collection) }
}

suspend fun <T> suspendMeasureTimeAndReturnResult(opName: String = "", block: suspend () -> T): T {
    val start = System.currentTimeMillis()
    val res = block()
    (System.currentTimeMillis() - start).let {
        if (it > 100) {
            println("$opName $it")
        }
    }
    return res
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