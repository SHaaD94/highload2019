package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.utils.now
import com.shaad.highload2018.utils.parsePhoneCode
import com.shaad.highload2018.web.get.FilterRequest
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

interface AccountRepository {
    fun addAccount(account: Account)
    fun filter(filterRequest: FilterRequest): List<Account>
}
//todo prefix tree for sname

class AccountRepositoryImpl : AccountRepository {
    private val accounts = ConcurrentHashMap<Int, Account>(10000)

    private val fnameIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val snameIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val emailDomainIndex = ConcurrentHashMap<String, MutableSet<Int>>()
    private val emailComparingIndex = ConcurrentSkipListMap<String, Int>()

    private val phoneCodeIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val countryIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val cityIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val birthIndex = ConcurrentSkipListMap<Long, Int>()

    private val interestIndex = ConcurrentHashMap<String, MutableSet<Int>>()

    private val likeIndex = ConcurrentHashMap<Int, MutableSet<Int>>()

    private val premiumIndex = ConcurrentHashMap<Int, Boolean>()


    override fun addAccount(account: Account) {
        withLockById(account.id) {
            check(accounts[account.id] == null) { "User ${account.id} already exists" }
            accounts[account.id] = account

            account.fname?.let {
                val collection = fnameIndex.computeIfAbsent(it) { LinkedHashSet() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }
            account.sname?.let {
                val collection = snameIndex.computeIfAbsent(it) { LinkedHashSet() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }
            account.email.let { email ->
                val domain = email.split("@").let { parsedEmail -> parsedEmail[parsedEmail.size - 1] }
                val domainCollection = emailDomainIndex.computeIfAbsent(domain) { LinkedHashSet() }
                synchronized(domainCollection) {
                    domainCollection.add(account.id)
                }

                emailComparingIndex.put(email, account.id)
            }
            account.phone?.let { phone ->
                val code = parsePhoneCode(phone)
                val codeCollection = phoneCodeIndex.computeIfAbsent(code) { LinkedHashSet() }
                synchronized(codeCollection) {
                    codeCollection.add(account.id)
                }
            }
            account.country?.let {
                val collection = countryIndex.computeIfAbsent(it) { LinkedHashSet() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }
            account.city?.let {
                val collection = cityIndex.computeIfAbsent(it) { LinkedHashSet() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }
            account.birth.let { birthIndex.put(it, account.id) }

            (account.interests ?: emptyList()).forEach {
                val collection = interestIndex.computeIfAbsent(it) { LinkedHashSet() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }
            (account.likes ?: emptyList()).forEach { (likeId, _) ->
                val collection = likeIndex.computeIfAbsent(likeId) { LinkedHashSet() }
                synchronized(collection) {
                    collection.add(account.id)
                }
            }

            account.premium?.let { (start, finish) ->
                premiumIndex.computeIfAbsent(account.id) { now() in (start until finish) }
            }
        }
    }

    override fun filter(filterRequest: FilterRequest): List<Account> {
        val ids: Collection<Int> = accounts.keys().toList()

        val filteredByEmail = filterRequest.email?.let { (domain, lt, gt) ->
            val filteredByDomain = if (domain != null) emailDomainIndex[domain] ?: emptyList() else ids
            val filteredByLt = if (lt != null) emailComparingIndex.headMap(lt).values else ids
            val filteredByGt = if (gt != null) emailComparingIndex.tailMap(gt).values else ids
            filteredByDomain.intersect(filteredByLt).intersect(filteredByGt)
        } ?: ids

        val filteredByFname = filterRequest.fname?.let { (eq, any, nill) ->
            val filteredByEq = if (eq != null) fnameIndex[eq] ?: emptyList() else ids
            val filteredByAny = if (any != null) any.flatMap { fnameIndex[eq] ?: emptyList<Int>() } else ids

            val result = filteredByEq.intersect(filteredByAny)
            filterByNull(nill, fnameIndex, result)
        } ?: ids

        val filteredBySname = filterRequest.sname?.let { (eq, starts, nill) ->
            val filteredByEq = if (eq != null) snameIndex[eq] ?: emptyList() else ids
            filterByNull(nill, snameIndex, filteredByEq)
        } ?: ids

        val filteredByPhone = filterRequest.phone?.let { (eq, nill) ->
            val filteredByEq = if (eq != null) phoneCodeIndex[eq] ?: emptyList() else ids
            filterByNull(nill, phoneCodeIndex, filteredByEq)
        } ?: ids

        val filteredByCountry = filterRequest.country?.let { (eq, nill) ->
            val filteredByEq = if (eq != null) countryIndex[eq] ?: emptyList() else ids
            filterByNull(nill, countryIndex, filteredByEq)
        } ?: ids

        val filteredByCity = filterRequest.city?.let { (eq, any, nill) ->
            val filteredByEq = if (eq != null) cityIndex[eq] ?: emptyList() else ids
            val filteredByAny = if (any != null) any.flatMap { cityIndex[eq] ?: emptyList<Int>() } else ids

            val result = filteredByEq.intersect(filteredByAny)
            filterByNull(nill, cityIndex, result)
        } ?: ids

        val filteredByBirth = filterRequest.birth?.let { (lt, gt, year) ->
            val filteredByLt = if (lt != null) birthIndex.headMap(lt).values else ids
            val filteredByGt = if (gt != null) birthIndex.tailMap(lt).values else ids
            val filteredByYear = if (year != null) {
                val from = LocalDateTime.of(year, 0, 0, 0, 0).toEpochSecond(ZoneOffset.UTC)
                val to = LocalDateTime.of(year + 1, 0, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) - 1
                birthIndex.tailMap(from).headMap(to).values
            } else ids
            filteredByLt.intersect(filteredByGt).intersect(filteredByYear)
        } ?: ids

        val filteredByInterests = filterRequest.interests?.let { (contains, any) ->
            val filteredByContains = contains?.let { interests ->
                interests.map { interestIndex[it] ?: emptyList<Int>() }.reduce { l, r -> l.intersect(r) }
            } ?: ids
            val filteredByAny = any?.let { interests ->
                interests.flatMap { interestIndex[it] ?: emptyList<Int>() }.toSet()
            } ?: ids
            filteredByContains.intersect(filteredByAny)
        } ?: ids

        val filteredByLikes = filterRequest.likes?.let { (contains) ->
            contains?.let { likes ->
                likes.map { likeIndex[it] ?: emptyList<Int>() }.reduce { l, r -> l.intersect(r) }
            } ?: ids
        } ?: ids

        val firstResult = filteredByEmail
            .intersect(filteredByFname)
            .intersect(filteredBySname)
            .intersect(filteredByPhone)
            .intersect(filteredByCountry)
            .intersect(filteredByCity)
            .intersect(filteredByBirth)
            .intersect(filteredByInterests)
            .intersect(filteredByLikes)

        val filteredByPremium = filterRequest.premium?.let { (now, nill) ->
            val filteredByNow = if (now == null) firstResult.filter { premiumIndex[it] == true } else firstResult
            if (nill != null) {
                //todo probably build in init block
                when (nill) {
                    true -> filteredByNow.filter { premiumIndex.contains(it) }
                    false -> filteredByNow.filter { !premiumIndex.contains(it) }
                }
            } else filteredByNow
        } ?: firstResult

        return filteredByPremium
            .asSequence()
            .map { accounts[it]!! }
            .filter { if (filterRequest.sex != null) it.sex == filterRequest.sex.eq else true }
            .filter {
                if (filterRequest.status != null) {
                    val eq = if (filterRequest.status.eq != null) it.status == filterRequest.status.eq else true
                    val neq = if (filterRequest.status.neq != null) it.status != filterRequest.status.neq else true
                    neq && eq
                } else true
            }
            .filter { if (filterRequest.sname?.starts != null) it.sname != null && it.sname.startsWith(filterRequest.sname.starts) else true }
            .take(filterRequest.limit)
            .toList()
    }

    private fun filterByNull(
        nill: Boolean?,
        index: Map<out Any, Collection<Int>>,
        collectionToFilter: Collection<Int>
    ): Collection<Int> {
        return if (nill != null) {
            //todo probably build in init block
            val allIdsWithName = index.flatMap { it.value }.toSet()
            when (nill) {
                true -> collectionToFilter.filter { allIdsWithName.contains(it) }
                false -> collectionToFilter.filter { !allIdsWithName.contains(it) }
            }
        } else collectionToFilter
    }

    private fun withLockById(id: Int, block: () -> Unit) = synchronized(id.toString().intern()) { block() }
}