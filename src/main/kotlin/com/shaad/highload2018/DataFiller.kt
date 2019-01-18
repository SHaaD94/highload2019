package com.shaad.highload2018


import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.addAccount
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipFile

private class Accounts(val accounts: List<Account>)

class DataFiller {
    private val dataPath = System.getProperty("shaad.tempdir") ?: "/tmp/data/data.zip"

    fun fill() = runBlocking {
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
        (0..2).map {
            GlobalScope.launch {
                for (id in accounts) {
                    addAccount(id)
                    if (counter.incrementAndGet() % 50_000 == 0) {
                        println("Processed $counter accounts")
                    }
                    if (counter.get() % 100_000 == 0) {
                        System.gc()
                    }
                }
            }
        }.joinAll()

    }
}