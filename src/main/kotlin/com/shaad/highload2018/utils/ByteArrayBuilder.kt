package com.shaad.highload2018.utils

import java.io.ByteArrayOutputStream

class ByteArrayBuilder(val size: Int? = null) {
    val stream = ByteArrayOutputStream(size ?: 32)

    companion object {
        private val intArray = Array(10) { it.toString().toByteArray() }
        val sexM = "m".toByteArray()
        val sexF = "f".toByteArray()
    }

    fun append(byteArray: ByteArray): ByteArrayBuilder {
        stream.write(byteArray)
        return this
    }

    fun append(int: Int): ByteArrayBuilder {
        var number = int.toDouble()
        var base = Math.pow(10.0, Math.log10(number).toInt().toDouble()).toInt()
        while (base > 0) {
            stream.write(intArray[(number / base).toInt()])
            number %= base
            base /= 10
        }
        return this
    }

    fun append(char: Char): ByteArrayBuilder {
        stream.write(if (char == 'm') sexM else sexF)
        return this
    }

    fun toArray(): ByteArray {
        val array = stream.toByteArray()
        stream.close()
        return array
    }
}