package com.shaad.highload2018

import com.squareup.okhttp.Callback
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.Response
import kotlinx.coroutines.*
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min
import kotlin.random.Random

fun main(args: Array<String>) {
    val localhost = "http://127.0.0.1:${System.getProperty("shaad.port") ?: 80}"

    val urls = listOf(
        "$localhost/accounts/filter/?city_any=Зеленодорф,Амстеровск,Волостан&sex_eq=f&interests_any=Рэп,Бокс,Целоваться&limit=4",
        "$localhost/accounts/filter/?limit=10&city_null=0&sex_eq=f&email_domain=mail.ru&premium_null=1&birth_year=1994&status_neq=1",
        "$localhost/accounts/filter/?sex_eq=m&birth_gt=773949382&country_null=0&status_neq=свободны&limit=10",
        "$localhost/accounts/group/?keys=status,city&order=-1&status=свободны&limit=50",
        "$localhost/accounts/group/?keys=sex,country&order=1&birth=1999&limit=10",
        "$localhost/accounts/group/?keys=interests,city&order=1&birth=1999&limit=10",
        "$localhost/accounts/10439/recommend/?city=Амстеродам&limit=20",
        "$localhost/accounts/11084/recommend/?country=Росмаль&limit=16",
//        "$localhost/accounts/10400/suggest/?limit=18&city=Барсостан",
        "$localhost/accounts/17399/suggest/?limit=16"
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
                client.setReadTimeout(2,TimeUnit.SECONDS)
                while (true) {
                    if (counter.get() > 100) {
                        continue
                    }
                    val url = urls[Random.nextInt(urls.size-1)]

                    val call = client.newCall(
                        Request.Builder().get().url(url)
                            .build()
                    )

                    counter.incrementAndGet()

//                    call.execute().let {
//                        it.body().close()
//                        if (it.code() != 200) {
//                            println("$url is not successful")
//                        }
//                    }

                    call.enqueue(object : Callback {
                        override fun onFailure(request: Request?, e: IOException?) {
                            println("$url is not successful")
                        }

                        override fun onResponse(response: Response?) {
                            response!!.body().close()
                        }
                    })
                }

            }
        }

    }
}