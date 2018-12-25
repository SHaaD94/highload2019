package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.Account

interface AccountRepository {
    fun addAccount(account: Account)
}

class AccountRepositoryImpl : AccountRepository {
    private val accounts = mutableListOf<Account>()

    @Synchronized
    override fun addAccount(account: Account) {
        accounts.add(account)
    }

}
