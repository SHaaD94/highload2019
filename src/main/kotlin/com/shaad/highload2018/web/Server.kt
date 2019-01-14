package com.shaad.highload2018.web

import com.google.inject.Inject
import org.rapidoid.buffer.Buf
import org.rapidoid.config.Conf
import org.rapidoid.http.AbstractHttpServer
import org.rapidoid.http.HttpStatus
import org.rapidoid.http.MediaType
import org.rapidoid.net.Server
import org.rapidoid.net.TCP
import org.rapidoid.net.abstracts.Channel
import org.rapidoid.net.impl.RapidoidHelper

class Server @Inject constructor(
    handlers: @JvmSuppressWildcards Set<Handler>
) : AbstractHttpServer() {
    private val method2Handler = handlers.groupBy { it.method().name }

    private val json = MediaType.create("application/json; charset=UTF-8;", "json", "map")

    override fun listen(address: String?, port: Int): Server =
        TCP.server(Conf.HTTP)
            .protocol(this)
            .blockingAccept(false)
            .noDelay(true)
            .syncBufs(false)
            .workers(4)
            .address(address)
            .port(port)
            .build()
            .start()


    override fun handle(ctx: Channel, buf: Buf, data: RapidoidHelper): HttpStatus {
        return try {
            val method = buf.get(data.verb)
            method2Handler[method]
                ?.firstOrNull { it.matches(buf, data.path) }
                ?.let { handler ->
                    kotlin.runCatching { handler.process(buf, data.path, data.query, data.body) }
                        .onSuccess {
                            startResponse(ctx, it.code, true)
                            writeBody(ctx, it.body, 0, it.body.size, json)
                        }
                        .onFailure { error ->
                            error.printStackTrace()
                            startResponse(ctx, 500, true)
                            writeBody(
                                ctx,
                                error.message?.toByteArray() ?: "Internal server error".toByteArray(),
                                0,
                                error.message?.toByteArray()?.size ?: 21,
                                json
                            )
                        }.getOrNull()
                    HttpStatus.DONE
                }
                ?: HttpStatus.NOT_FOUND
        } catch (e: Throwable) {
            e.printStackTrace()
            HttpStatus.ERROR
        }
    }
}
