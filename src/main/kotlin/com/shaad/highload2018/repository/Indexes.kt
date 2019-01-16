package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.InnerAccount
import org.agrona.collections.IntArrayList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

// indexes
val accounts0_250 = Array<InnerAccount?>(250_000) { null }
val accounts250_500 = Array<InnerAccount?>(250_000) { null }
val accounts500_750 = Array<InnerAccount?>(250_000) { null }
val accounts750_1000 = Array<InnerAccount?>(250_000) { null }
val accounts1000_1300 = Array<InnerAccount?>(300_000) { null }
val accounts1300 = ConcurrentHashMap<Int, InnerAccount>(30_000)

val statusIndex = Array(4) { Array(20) { IntArrayList() } }

val sexIndex = Array(2) { Array(20) { IntArrayList() } }

val fnameIndex = Array(1000) { Array(20) { IntArrayList() } }

val snameIndex = Array(20000) { Array(20) { IntArrayList() } }

val emailDomainIndex = ConcurrentHashMap<String, Array<IntArrayList>>()
val emailIndex = Array(36) { ConcurrentHashMap<String, Int>() }

val phoneCodeIndex = Array(1000) { Array(20) { IntArrayList() } }

val countryIndex = Array(100) { Array(20) { IntArrayList() } }

val cityIndex = Array(1000) { Array(20) { IntArrayList() } }

val birthIndex = Array(100) { Array(20) { IntArrayList() } }

val joinedIndex = Array(10) { Array(20) { IntArrayList() } }

val interestIndex = Array(200) { Array(20) { IntArrayList() } }

val premiumNowIndex = Array(20) { IntArrayList() }

val likeIndex0_250 = Array<IntArrayList>(250_000) { IntArrayList() }
val likeIndex250_500 = Array<IntArrayList>(250_000) { IntArrayList() }
val likeIndex500_750 = Array<IntArrayList>(250_000) { IntArrayList() }
val likeIndex750_1000 = Array<IntArrayList>(250_000) { IntArrayList() }
val likeIndex1000_1300 = Array<IntArrayList>(300_000) { IntArrayList() }
val likeIndex1300 = ConcurrentHashMap<Int, IntArrayList>(30_000)

//normalization entities
val citiesIdCounter = AtomicInteger()
val cities = ConcurrentHashMap<String, Int>()
val citiesInv = ConcurrentHashMap<Int, String>()

val countriesIdCounter = AtomicInteger()
val countries = ConcurrentHashMap<String, Int>()
val countriesInv = ConcurrentHashMap<Int, String>()

val interestsIdCounter = AtomicInteger()
val interests = ConcurrentHashMap<String, Int>()
val interestsInv = ConcurrentHashMap<Int, String>()

val statusesIdCounter = AtomicInteger()
val statuses = ConcurrentHashMap<String, Int>()
val statusesInv = ConcurrentHashMap<Int, String>()

val fnamesIdCounter = AtomicInteger()
val fnames = ConcurrentHashMap<String, Int>()
val fnamesInv = ConcurrentHashMap<Int, String>()

val snamesIdCounter = AtomicInteger()
val snames = ConcurrentHashMap<String, Int>()
val snamesInv = ConcurrentHashMap<Int, String>()


fun getIdBucket(id: Int, array: Array<IntArrayList>): IntArrayList = when {
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

fun getLikesByIndex(likeId: Int): IntArrayList? =
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
    counter: AtomicInteger,
    property: String
): Int {
    return index.computeIfAbsent(property) {
        val id = counter.incrementAndGet()
        invIndex[id] = property
        id
    }
}

fun int2Sex(int: Int) = when (int) {
    0 -> 'm'
    1 -> 'f'
    else -> throw RuntimeException("Unknown sex $int")
}

fun sex2Int(sex: Char) = if (sex == 'm') 0 else 1