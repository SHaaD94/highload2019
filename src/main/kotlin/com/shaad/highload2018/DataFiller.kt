package com.shaad.highload2018


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.inject.Inject
import com.shaad.highload2018.domain.Account
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File

private class Accounts(val accounts: List<Account>)

class DataFiller @Inject constructor(private val accountRepository: AccountRepository) {
    private val dataPath = "/tmp/data/data.zip"
    private val tempDir =
        System.getProperty("shaad.temp.dir.path") ?: throw RuntimeException("Please specify -Dshaad.temp.dir.path")

    fun fill() {
        Runtime.getRuntime().exec("mkdir -p $tempDir").waitFor()
        File(tempDir).listFiles().filter { !it.isDirectory && it.name.startsWith("accounts") }.forEach { it.delete() }
        println("Unzipping")
        Runtime.getRuntime().exec("unzip -o $dataPath -d $tempDir").waitFor()
        println("Reading features")

        val objectMapper = jacksonObjectMapper()
            .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

        runBlocking {
            val accounts = Channel<Account>(Channel.UNLIMITED)
            GlobalScope.launch {
                File(tempDir).listFiles().forEach { entry ->
                    measureTimeAndReturnResult("finished file ${entry.name} in") {
                        objectMapper.readValue<Accounts>(entry, Accounts::class.java)
                    }.accounts.forEach { check(accounts.offer(it)) }
                }
                accounts.close()
            }

            GlobalScope.launch {
                var counter = 0
                for (id in accounts) {
                    accountRepository.addAccount(id)
                    counter++
                    if (counter % 50_000 == 0) println("Processed $counter accounts")
                }
            }.join()
        }

    }
}