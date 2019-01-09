package com.shaad.highload2018

import com.google.inject.Guice
import com.google.inject.Stage
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.web.Server

fun main(args: Array<String>) {
    println(System.currentTimeMillis())
    val injector = measureTimeAndReturnResult("Created injector in") {
        Guice.createInjector(
            Stage.PRODUCTION,
            BaseModule()
        )
    }
    measureTimeAndReturnResult("Filling is finished in") {
        injector.getInstance(DataFiller::class.java).fill()
    }

    System.gc()

    injector.getInstance(Server::class.java)
        .listen(80)

    System.gc()
}
