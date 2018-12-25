package com.shaad.highload2018.web

import org.rapidoid.buffer.Buf
import org.rapidoid.http.AbstractHttpServer
import org.rapidoid.http.HttpStatus
import org.rapidoid.http.MediaType
import org.rapidoid.net.abstracts.Channel
import org.rapidoid.net.impl.RapidoidHelper

class Server(handlers: @JvmSuppressWildcards Set<Handler>) : AbstractHttpServer() {
    private val method2Handler = handlers.groupBy { it.method().name }

    private val json = MediaType.create("application/json; charset=UTF-8;", "json", "map")

    override fun handle(ctx: Channel, buf: Buf, data: RapidoidHelper): HttpStatus {
        val method = buf.get(data.verb)
        return method2Handler[method]
            ?.firstOrNull { it.matches(buf, data.path) }
            ?.process(buf, data.path, data.query)
            ?.let {
                ok(ctx, true, it, json)
                HttpStatus.DONE
            }
            ?: HttpStatus.NOT_FOUND
    }
}
