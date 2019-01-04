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

fun <K> concurrentHashSet() : MutableSet<K> = ConcurrentHashMap.newKeySet()

fun now() = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)