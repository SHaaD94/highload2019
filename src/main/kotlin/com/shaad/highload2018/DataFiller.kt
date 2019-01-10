package com.shaad.highload2018


import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.inject.Inject
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.AccountRepository
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipFile

private class Accounts(val accounts: List<Account>)

class DataFiller @Inject constructor(private val accountRepository: AccountRepository) {
    private val dataPath = System.getProperty("shaad.tempdir") ?: "/tmp/data/data.zip"

    fun fill() {
        runBlocking {
            val accounts = Channel<Account>(10_000)
            GlobalScope.launch {
                val objectMapper = jacksonObjectMapper()

                ZipFile(dataPath).use { zip ->
                    val iterator = zip.entries()
                    while (iterator.hasMoreElements()) {
                        val entry = iterator.nextElement()
                        objectMapper.readValue<Accounts>(zip.getInputStream(entry), Accounts::class.java)
                            .accounts.forEach { acc -> accounts.send(acc) }
                    }
                }
                println("All files read")
                accounts.close()
            }

            val counter = AtomicInteger(0)

            GlobalScope.launch {
                for (id in accounts) {
                    accountRepository.addAccount(id)
                    if (counter.incrementAndGet() % 50_000 == 0) {
                        println("Processed $counter accounts")
                    }
                    if (counter.get() % 300_000 == 0) {
                        System.gc()
                    }
                }
            }.join()
        }
    }
}