package com.shaad.highload2018


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.inject.Inject
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.utils.suspendMeasureTimeAndReturnResult
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipFile

private class Accounts(val accounts: List<Account>)

class DataFiller @Inject constructor(private val accountRepository: AccountRepository) {
    private val dataPath = "/tmp/data/data.zip"

    fun fill() {
        runBlocking {
            val accounts = Channel<Account>(200_000)
            GlobalScope.launch {
                val objectMapper = jacksonObjectMapper()
                    .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

                ZipFile(dataPath).use { zip ->
                    for (entry in zip.entries()) {
                        suspendMeasureTimeAndReturnResult("read file ${entry.name} in") {
                            objectMapper.readValue<Accounts>(zip.getInputStream(entry), Accounts::class.java)
                                .accounts.forEach { acc ->
                                accounts.send(acc)
                            }
                        }
                    }
                }
                println("All files read")
                accounts.close()
            }

            val counter = AtomicInteger(0)

            GlobalScope.launch {
                for (id in accounts) {
                    accountRepository.addAccount(id)
                    if (counter.incrementAndGet() % 50_000 == 0) println("Processed $counter accounts")
                }
            }.join()
        }
    }
}