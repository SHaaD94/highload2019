package com.shaad.highload2018

import com.squareup.okhttp.MediaType
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.RequestBody
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class Heater {
    private val client = OkHttpClient()
    fun warmUp() {
        client.setConnectTimeout(2, TimeUnit.MINUTES)
        client.setReadTimeout(2, TimeUnit.MINUTES)
        client.setWriteTimeout(2, TimeUnit.MINUTES)

        val localhost = "http://127.0.0.1"

        listOf(
            "$localhost/accounts/filter/?city_any=Зеленодорф,Амстеровск,Волостан&sex_eq=f&interests_any=Рэп,Бокс,Целоваться&limit=4",
            "$localhost/accounts/filter/?limit=10&city_null=0&sex_eq=f&email_domain=mail.ru&premium_null=1&birth_year=1994&status_neq=1",
            "$localhost/accounts/filter/?sex_eq=m&birth_gt=773949382&country_null=0&status_neq=свободны&limit=10",
            "$localhost/accounts/group/?keys=city&order=-1&status=свободны&limit=50",
            "$localhost/accounts/group/?keys=interests&order=1&birth=1999&limit=10",
            "$localhost/accounts/10048/suggest/?city=Рособирск&limit=10",
            "$localhost/accounts/18111/suggest/?country=Росизия&limit=12",
            "$localhost/accounts/10439/recommend/?city=Амстеродам&limit=20",
            "$localhost/accounts/11084/recommend/?country=Росмаль&limit=16"
        ).forEach { url -> repeat(1000) { get(url) } }
//
        listOf(
            "$localhost/accounts/10178/?query_id=18",
            "$localhost/accounts/10544/?query_id=8",
            "$localhost/accounts/likes/?query_id=11"
        ).forEach { url -> repeat(10) { post(url) } }

        println("Warmed up!")
    }

    private fun get(url: String) {
        Request.Builder().url(url).method("GET", null).build()
            .let {
                client.newCall(it).execute().let {
                    it.body().close()
                    it.code()
                }
            }.let { if (it != 200) println("WARN code is not 200!!! $url ") }
    }

    private fun post(url: String) {
        Request.Builder().url(url).method("POST", RequestBody.create(MediaType.parse("application/json"), "{}")).build()
            .let {
                client.newCall(it).execute().let {
                    it.body().close()
                }
            }
    }
}