package com.shaad.highload2018.domain

data class Account(
    val id: Int?,
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
    val birth: Int,
    // max 50 symbols
    val country: String?,
    // max 50 symbols
    val city: String?,

    //------------

    // from 01.01.2011 to 01.01.2018.
    val joined: Int,

    //"свободны", "заняты", "всё сложно"
    val status: String,

    //max 100 symbols each
    val interests: List<String>?,

    val premium: Premium?,

    val likes: List<Like>?
)

//min 01.01.2018
data class Premium(val start: Int, val finish: Int) {
    init {
        check(start < finish)
    }
}

data class Like(val id: Int?, val ts: Int)

data class InnerAccount(
    val id: Int?,
    val status: Int?,
    val email: String,
    val sex: Int?,
    val fname: Int?,
    val sname: Int?,
    val city: Int?,
    val country: Int?,
    val birth: Int,
    val phone: String?,
    val premium: Premium?,
    val interests: List<Int>?/*,
    val likes: List<InnerLike>?*/
)

data class InnerLike(val id: Int?, val days: Int?, val ms: Int?) {
    constructor(like: Like) : this(like.id, like.ts / 86400, like.ts % 86400)
}