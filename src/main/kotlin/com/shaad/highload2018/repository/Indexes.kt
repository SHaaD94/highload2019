package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.InnerAccount
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

// indexes
val accounts0_250 = Array<InnerAccount?>(250_000) { null }
val accounts250_500 = Array<InnerAccount?>(250_000) { null }
val accounts500_750 = Array<InnerAccount?>(250_000) { null }
val accounts750_1000 = Array<InnerAccount?>(250_000) { null }
val accounts1000_1300 = Array<InnerAccount?>(300_000) { null }
val accounts1300 = ConcurrentHashMap<Int, InnerAccount>(100_000)

val statusIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

val sexIndex = ConcurrentHashMap<Char, Array<ArrayList<Int>>>()

val fnameIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

val snameIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

val emailDomainIndex = ConcurrentHashMap<String, Array<ArrayList<Int>>>()
val emailIndex = Array(36) { ConcurrentHashMap<String, Int>() }

val phoneCodeIndex = Array(1000) { Array(20) { ArrayList<Int>() } }

val countryIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

val cityIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

val birthIndex = Array(100) { Array(20) { ArrayList<Int>() } }

val joinedIndex = Array(10) { Array(20) { ArrayList<Int>() } }

val interestIndex = ConcurrentHashMap<Int, Array<ArrayList<Int>>>()

val premiumNowIndex = Array(20) { ArrayList<Int>() }

val likeIndex0_250 = Array<ArrayList<Int>>(250_000) { ArrayList() }
val likeIndex250_500 = Array<ArrayList<Int>>(250_000) { ArrayList() }
val likeIndex500_750 = Array<ArrayList<Int>>(250_000) { ArrayList() }
val likeIndex750_1000 = Array<ArrayList<Int>>(250_000) { ArrayList() }
val likeIndex1000_1300 = Array<ArrayList<Int>>(300_000) { ArrayList() }
val likeIndex1300 = ConcurrentHashMap<Int, ArrayList<Int>>(100_000)

//normalization entities
val idCounter = AtomicInteger()
val cities = ConcurrentHashMap<String, Int>()
val citiesInv = ConcurrentHashMap<Int, String>()
val countries = ConcurrentHashMap<String, Int>()
val countriesInv = ConcurrentHashMap<Int, String>()
val interests = ConcurrentHashMap<String, Int>()
val interestsInv = ConcurrentHashMap<Int, String>()
val statuses = ConcurrentHashMap<String, Int>()
val statusesInv = ConcurrentHashMap<Int, String>()
val fnames = ConcurrentHashMap<String, Int>()
val fnamesInv = ConcurrentHashMap<Int, String>()
val snames = ConcurrentHashMap<String, Int>()
val snamesInv = ConcurrentHashMap<Int, String>()


fun getIdBucket(id: Int, array: Array<ArrayList<Int>>): ArrayList<Int> = when {
    id < 100_000 -> 0
    id in 100_000 until 200_000 -> 1
    id in 200_000 until 300_000 -> 2
    id in 300_000 until 400_000 -> 3
    id in 400_000 until 500_000 -> 4
    id in 500_000 until 600_000 -> 5
    id in 600_000 until 700_000 -> 6
    id in 700_000 until 800_000 -> 7
    id in 800_000 until 900_000 -> 8
    id in 900_000 until 1000_000 -> 9
    id in 1000_000 until 1100_000 -> 10
    id in 1100_000 until 1200_000 -> 11
    id in 1200_000 until 1300_000 -> 12
    id in 1300_000 until 1400_000 -> 13
    id in 1400_000 until 1500_000 -> 14
    id in 1500_000 until 1600_000 -> 15
    id in 1600_000 until 1700_000 -> 16
    id in 1700_000 until 1800_000 -> 17
    id in 1800_000 until 1900_000 -> 18
    else -> 19
}.let { array[it] }

fun getLexIndex(char: Char) = when (char) {
    '0' -> 0
    '1' -> 1
    '2' -> 2
    '3' -> 3
    '4' -> 4
    '5' -> 5
    '6' -> 6
    '7' -> 7
    '8' -> 8
    '9' -> 9
    'a' -> 10
    'b' -> 11
    'c' -> 12
    'd' -> 13
    'e' -> 14
    'f' -> 15
    'g' -> 16
    'h' -> 17
    'i' -> 18
    'j' -> 19
    'k' -> 20
    'l' -> 21
    'm' -> 22
    'n' -> 23
    'o' -> 24
    'p' -> 25
    'q' -> 26
    'r' -> 27
    's' -> 28
    't' -> 29
    'u' -> 30
    'v' -> 31
    'w' -> 32
    'x' -> 33
    'y' -> 34
    'z' -> 35
    else -> throw RuntimeException("Unknown char $char")
}

fun getLikesByIndex(likeId: Int): ArrayList<Int>? =
    when {
        likeId < 250_000 -> likeIndex0_250[likeId]
        likeId in 250_000 until 500_000 -> likeIndex250_500[likeId - 250_000]
        likeId in 500_000 until 750_000 -> likeIndex500_750[likeId - 500_000]
        likeId in 750_000 until 1_000_000 -> likeIndex750_1000[likeId - 750_000]
        likeId in 1_000_000 until 1_300_000 -> likeIndex1000_1300[likeId - 1_000_000]

        else -> likeIndex1300[likeId]
    }

fun getAccountByIndex(id: Int): InnerAccount? = when {
    id < 250_000 -> accounts0_250[id]
    id in 250_000 until 500_000 -> accounts250_500[id - 250_000]
    id in 500_000 until 750_000 -> accounts500_750[id - 500_000]
    id in 750_000 until 1_000_000 -> accounts750_1000[id - 750_000]
    id in 1_000_000 until 1_300_000 -> accounts1000_1300[id - 1_000_000]
    else -> accounts1300[id]
}

fun addEmailToIndex(email: String, id: Int) {
    emailIndex[getLexIndex(email[0])].compute(email) { _, existingId ->
        if (existingId != null) throw RuntimeException("email $email already binded")
        else id
    }
}

fun writeNormalizationIndex(
    index: ConcurrentHashMap<String, Int>,
    invIndex: ConcurrentHashMap<Int, String>,
    property: String
): Int {
    return index.computeIfAbsent(property) {
        val id = idCounter.incrementAndGet()
        invIndex[id] = property
        id
    }
}

fun int2Sex(int: Int?) = when (int) {
    0 -> 'm'
    1 -> 'f'
    else -> null
}
