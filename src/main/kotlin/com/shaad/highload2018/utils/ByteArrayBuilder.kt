package com.shaad.highload2018.utils

import java.io.ByteArrayOutputStream

class ByteArrayBuilder(val size: Int? = null) {
    val stream = ByteArrayOutputStream(size ?: 32)

    fun append(byteArray: ByteArray): ByteArrayBuilder {
        stream.write(byteArray)
        return this
    }

    fun toArray(): ByteArray {
        val array = stream.toByteArray()
        stream.close()
        return array
    }
}