package com.shaad.highload2018.configuration

import com.google.inject.AbstractModule
import com.google.inject.Singleton
import com.google.inject.multibindings.Multibinder
import com.shaad.highload2018.repository.AccountRepository
import com.shaad.highload2018.repository.AccountRepositoryImpl
import com.shaad.highload2018.web.Handler
import com.shaad.highload2018.web.get.AccountsFilter
import com.shaad.highload2018.web.get.AccountsGroup
import com.shaad.highload2018.web.get.AccountsRecommend
import com.shaad.highload2018.web.get.AccountsSuggest
import com.shaad.highload2018.web.post.AccountsLikes
import com.shaad.highload2018.web.post.AccountsNew
import com.shaad.highload2018.web.post.AccountsUpdate

class BaseModule : AbstractModule() {
    override fun configure() {
        val multibinder = Multibinder.newSetBinder(binder(), Handler::class.java)
        multibinder.addBinding().to(AccountsFilter::class.java)
        multibinder.addBinding().to(AccountsGroup::class.java)
        multibinder.addBinding().to(AccountsRecommend::class.java)
        multibinder.addBinding().to(AccountsSuggest::class.java)
        multibinder.addBinding().to(AccountsLikes::class.java)
        multibinder.addBinding().to(AccountsNew::class.java)
        multibinder.addBinding().to(AccountsUpdate::class.java)

        bind(AccountRepository::class.java).to(AccountRepositoryImpl::class.java).`in`(Singleton::class.java)
    }
}
