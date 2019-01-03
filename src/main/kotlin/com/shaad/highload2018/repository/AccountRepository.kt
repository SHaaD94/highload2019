package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.utils.now
import com.shaad.highload2018.utils.parsePhoneCode
import com.shaad.highload2018.web.get.FilterRequest
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
            check(accounts[account.id] != null) { "User ${account.id} already exists" }
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
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun withLockById(id: Int, block: () -> Unit) = synchronized(id.toString().intern()) { block() }
}