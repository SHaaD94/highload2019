package com.shaad.highload2018.domain

class Account(
    val id: Int,
    // max 100 symbols, unique
    val emain: ByteArray,
    // max 50 symbols
    val fname: ByteArray?,
    val sname: ByteArray?,
    // max 16 symbols unique
    val phone: ByteArray?,
    // m or f
    val sex: Char,
    // from 01.01.1950 to 01.01.2005
    val birth: Long,
    // max 50 symbols
    val country: ByteArray?,
    // max 50 symbols
    val city: ByteArray?,

    //------------

    // from 01.01.2011 to 01.01.2018.
    val joined: Long,

    //"свободны", "заняты", "всё сложно"
    val status: ByteArray,

    //max 100 symbols each
    val interests: List<ByteArray>,

    val premium: Premium?,

    val likes: List<Like>
)

//min 01.01.2018
class Premium(val start: Long, val finish: Long) {
    init {
        check(start < finish)
    }
}

class Like(val id: Int, val ts: Long)