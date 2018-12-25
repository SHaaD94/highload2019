package com.shaad.highload2018

import com.google.inject.Guice
import com.shaad.highload2018.configuration.BaseModule
import com.shaad.highload2018.web.Server

fun main(args: Array<String>) {
    Guice.createInjector(
        BaseModule()
    )
        .getInstance(Server::class.java)
        .listen(80)
}