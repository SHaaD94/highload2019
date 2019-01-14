package com.shaad.highload2018.utils

import com.google.common.primitives.UnsignedBytes

fun main(args: Array<String>) {
    val lexComparator = UnsignedBytes.lexicographicalComparator()
    val f = "osasahsays@rambler.ru"
    val s = "efebrinvi@icloud.com"
    val ten = "ten"
    println(lexComparator.compare(f.toByteArray(), ten.toByteArray()))
    println(lexComparator.compare(s.toByteArray(), ten.toByteArray()))
    println(compareEmail(f.toByteArray(), ten.toByteArray()))
    println(compareEmail(s.toByteArray(), ten.toByteArray()))
    println(f > ten)
    println(s > ten)
}

fun compareEmail(email: ByteArray, pattern: ByteArray): Int {
    if (pattern.isEmpty()) {
        return 0
    }
    var counter = 0
    while (counter < email.size && counter < pattern.size && email[counter] == pattern[counter]) {
        counter++
    }

    return email[counter] - pattern[Math.max(counter - 1, 0)]
}