package com.shaad.highload2018


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.inject.Inject
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

private class Accounts(val accounts: List<Account>)

class DataFiller @Inject constructor(private val accountRepository: AccountRepository) {
    private val dataPath = "/tmp/data/data.zip"

    fun fill() {
        val dispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

        ZipFile(dataPath).use { zip ->
            runBlocking {
                val zipEntries = Channel<ZipEntry>(4)

                launch {
                    for (entry in zip.entries()) {
                        zipEntries.send(entry)
                    }

                    zipEntries.close()
                }

                withContext(dispatcher) {
                    repeat(4) {
                        readingWorker(zip, zipEntries)
                    }
                }
            }
        }

        (dispatcher.executor as ExecutorService).shutdown()
    }

    private fun CoroutineScope.readingWorker(zipFile: ZipFile, entries: Channel<ZipEntry>) = launch {
        val objectMapper = jacksonObjectMapper()
            .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

        for (entry in entries) {
            measureTimeAndReturnResult("finished file ${entry.name} in") {
                zipFile.getInputStream(entry).use {
                    objectMapper.readValue<Accounts>(it, Accounts::class.java)
                }.accounts.forEach {
                    accountRepository.addAccount(it)
                }
            }
        }
    }
}