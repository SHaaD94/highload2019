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

fun getYear(timestamp: Int): Int {
    return LocalDateTime.ofEpochSecond(timestamp.toLong(), 0, moscowTimeZone).year
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
