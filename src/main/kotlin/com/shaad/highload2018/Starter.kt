package com.shaad.highload2018

import com.google.inject.Guice
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.web.Server
import org.rapidoid.util.Msc

fun main(args: Array<String>) {
    val injector = Guice.createInjector(
        BaseModule()
    )
    injector.getInstance(DataFiller::class.java).fill()
    injector.getInstance(Server::class.java)
        .listen(80)
}
