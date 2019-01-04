package com.shaad.highload2018.domain

import com.fasterxml.jackson.annotation.JsonIgnore

data class Account(
    val id: Int,
    // max 100 symbols, unique
    val email: String,
    // max 50 symbols
    val fname: String?,
    val sname: String?,
    // max 16 symbols unique
    val phone: String?,
    // m or f
    val sex: Char,
    // from 01.01.1950 to 01.01.2005
    val birth: Long,
    // max 50 symbols
    val country: String?,
    // max 50 symbols
    val city: String?,

    //------------

    // from 01.01.2011 to 01.01.2018.
    val joined: Long,

    //"свободны", "заняты", "всё сложно"
    val status: String,

    //max 100 symbols each
    val interests: List<String>?,

    val premium: Premium?,

    val likes: List<Like>?
)

//min 01.01.2018
data class Premium(val start: Long, val finish: Long) {
    init {
        check(start < finish)
    }
}

data class Like(val id: Int, val ts: Long)