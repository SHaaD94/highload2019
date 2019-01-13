package com.shaad.highload2018

import com.google.inject.Guice
import com.google.inject.Stage
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.web.Server
import kotlin.concurrent.fixedRateTimer

fun main(args: Array<String>) {
    val injector = Guice.createInjector(
        Stage.PRODUCTION,
        BaseModule()
    )

    fixedRateTimer("dump memory", false, 0, 1_000) {
        println("Current free memory is ${Runtime.getRuntime().freeMemory()} of ${Runtime.getRuntime().totalMemory()} (max ${Runtime.getRuntime().maxMemory()})")
    }

    measureTimeAndReturnResult("Filling is finished in") {
        injector.getInstance(DataFiller::class.java).fill()
    }

    System.gc()

    injector.getInstance(Server::class.java)
        .listen(System.getProperty("shaad.port")?.toInt() ?: 80)

    System.gc()

    Heater().warmUp()

    System.gc()
}
