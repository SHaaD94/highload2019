package com.shaad.highload2018

import com.squareup.okhttp.Callback
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.Response
import kotlinx.coroutines.*
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

fun main(args: Array<String>) {
    val localhost = "http://127.0.0.1:8080"

    val urls = listOf(
        "$localhost/accounts/filter/?city_any=Зеленодорф,Амстеровск,Волостан&sex_eq=f&interests_any=Рэп,Бокс,Целоваться&limit=4",
        "$localhost/accounts/filter/?limit=10&city_null=0&sex_eq=f&email_domain=mail.ru&premium_null=1&birth_year=1994&status_neq=1",
        "$localhost/accounts/filter/?sex_eq=m&birth_gt=773949382&country_null=0&status_neq=свободны&limit=10",
        "$localhost/accounts/group/?keys=city&order=-1&status=свободны&limit=50",
        "$localhost/accounts/group/?keys=interests&order=1&birth=1999&limit=10"
    )

    val context = Executors.newFixedThreadPool(64).asCoroutineDispatcher()

    val counter = AtomicInteger(0)
    runBlocking {

        GlobalScope.launch {
            while (true) {
                println("RPS IS ${counter.getAndSet(0)}")
                delay(999)
            }
        }
        repeat(64) { number ->
            launch(context) {
                val client = OkHttpClient()
                while (true) {
                    if (counter.get() > 1_000) {
                        continue
                    }
                    val call = client.newCall(
                        Request.Builder().get().url(urls[number % urls.size - 1])
                            .build()
                    )

                    counter.incrementAndGet()
                    call.execute().let {
                        it.body().close()
                        if (it.code()!=200){
                            println(urls[number % urls.size - 1] + " is not successful")
                        }
                    }
//                    call.enqueue(object : Callback {
//                        override fun onFailure(request: Request?, e: IOException?) {
//                            println(urls[number % urls.size - 1] + " is not successful")
//                        }
//
//                        override fun onResponse(response: Response?) {
//                            response!!.body().close()
//                        }
//                    })
                }

            }
        }

    }
}