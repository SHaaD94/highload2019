package com.shaad.highload2018

import com.google.inject.Guice
import com.google.inject.Stage
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.utils.measureTimeAndReturnResult
import com.shaad.highload2018.web.Server
import com.wizzardo.tools.json.JsonObject
import java.lang.management.ManagementFactory
import kotlin.concurrent.fixedRateTimer


fun main(args: Array<String>) {
    val injector = Guice.createInjector(
        Stage.PRODUCTION,
        BaseModule()
    )

    fixedRateTimer("dump memory", false, 1_000, 5_000) {
        println("-----------------------")
        println(getMemoryUsage())
        println("-----------------------")
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


fun getMemoryUsage(): String {
    val json = JsonObject()
    var totalCommited: Long = 0
    var totalUsed: Long = 0
    for (pool in ManagementFactory.getMemoryPoolMXBeans()) {
        val usage = pool.usage
        totalCommited += usage.committed
        totalUsed += usage.used
        json.append(
            pool.name, JsonObject()
                .append("commited", usage.committed / 1024 / 1024)
                .append("used", usage.used / 1024 / 1024)
        )
    }

    json.append(
        "total", JsonObject()
            .append("commited", totalCommited / 1024 / 1024)
            .append("used", totalUsed / 1024 / 1024)
    )
    val memoryMXBean = ManagementFactory.getMemoryMXBean()

    json.append(
        "MemoryMXBean.heap", JsonObject()
            .append("commited", memoryMXBean.heapMemoryUsage.committed / 1024 / 1024)
            .append("used", memoryMXBean.heapMemoryUsage.used / 1024 / 1024)
    )
    json.append(
        "MemoryMXBean.nonheap", JsonObject()
            .append("commited", memoryMXBean.nonHeapMemoryUsage.committed / 1024 / 1024)
            .append("used", memoryMXBean.nonHeapMemoryUsage.used / 1024 / 1024)
    )
    return json.toString()
}