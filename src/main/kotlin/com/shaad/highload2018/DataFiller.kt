package com.shaad.highload2018


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.inject.Inject
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.AccountRepository
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipFile

private class Accounts(val accounts: List<Account>)

class DataFiller @Inject constructor(private val accountRepository: AccountRepository) {
    private val dataPath = "/tmp/data/data.zip"

    fun fill() {
        val objectMapper = jacksonObjectMapper()
            .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

        ZipFile(dataPath).use { zip ->
            for (file in zip.entries()) {
                zip.getInputStream(file).use {
                    objectMapper.readValue<Accounts>(it, Accounts::class.java)
                }.accounts.forEach {
                    accountRepository.addAccount(it)
                }
            }
        }
    }
}