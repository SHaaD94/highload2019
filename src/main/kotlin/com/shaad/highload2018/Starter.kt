package com.shaad.highload2018

import com.google.inject.Guice
import com.google.inject.Stage
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.web.Server
import java.util.concurrent.TimeUnit
import kotlin.concurrent.fixedRateTimer
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val injector = Guice.createInjector(
        Stage.PRODUCTION,
        BaseModule()
    )

    fixedRateTimer("dump memory", false, 1_000, 5_000) {
        ProcessBuilder("awk '/^Mem/ {printf(\"%u%%\", 100*\$3/\$2);}' <(free -m)")
            .start()
            .inputStream.reader().readText().let { "Used memory $it" }
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
