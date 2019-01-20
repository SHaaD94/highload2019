package com.shaad.highload2018.repository

import com.shaad.highload2018.utils.addToSortedCollection
import org.agrona.collections.IntArrayList

fun addLike(likee: Int, ts: Int, liker: Int) {
    val account = getAccountByIndex(liker)!!
    check(getAccountByIndex(likee) != null) { "Likee does not exist" }
    if (account.likes == null) {
        account.likes = IntArrayList()
    }
    account.likes!!.addInt(likee)
    if (account.likeTs == null) {
        account.likeTs = IntArrayList()
    }
    account.likeTs!!.addInt(ts)

    var collection = when {
        likee < 1_300_000 -> likeIndex[(likee / 50_000)][likee % 50_000]
        else -> null
    }
    if (collection == null) {
        collection = likeIndex1300.computeIfAbsent(likee) { IntArrayList() }
    }

    addToSortedCollection(collection, account.id)
}