package com.shaad.highload2018

import com.google.inject.Guice
import com.google.inject.Stage
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.repository.citiesIdCounter
import com.shaad.highload2018.repository.countriesIdCounter
import com.shaad.highload2018.repository.fnamesIdCounter
import com.shaad.highload2018.repository.snamesIdCounter
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.web.Server
import java.io.File
import java.nio.charset.Charset
import kotlin.concurrent.fixedRateTimer


fun main(args: Array<String>) {
    val injector = Guice.createInjector(
        Stage.PRODUCTION,
        BaseModule()
    )

    fixedRateTimer("free memory monitor", true, 0, 5_000) {
        try {
//            println("UsedMemory: ${File("/sys/fs/cgroup/memory/memory.usage_in_bytes").readText().toLong() / 1024.0 / 1024.0}")
//            println("UsedMemory: ${File("/proc/meminfo").inputStream().readBytes().toString(Charset.defaultCharset())}")
            println("UsedMemory: ${File("/proc/self/stat").inputStream().readBytes().toString(Charset.defaultCharset())}")
        }catch (e:Exception){
            e.printStackTrace()
        }
    }

    measureTimeAndReturnResult("Filling is finished in") {
        injector.getInstance(DataFiller::class.java).fill()
    }

    System.gc()

    println("--------")
    println("Stats:")
    println("Total cities: ${citiesIdCounter.get()}")
    println("Total countries: ${countriesIdCounter.get()}")
    println("Total interests: ${citiesIdCounter.get()}")
    println("Total fnames: ${fnamesIdCounter.get()}")
    println("Total snames: ${snamesIdCounter.get()}")
    println("--------")

    injector.getInstance(Server::class.java)
        .listen(System.getProperty("shaad.port")?.toInt() ?: 80)

    System.gc()

    measureTimeAndReturnResult("Warmed up in") {
        Heater().warmUp()
    }

    System.gc()
}
