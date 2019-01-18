package com.shaad.highload2018.repository

import com.shaad.highload2018.domain.InnerAccount

fun suggest(id: Int, city: String?, country: String?, limit: Int): Sequence<InnerAccount> {
    val account = getAccountByIndex(id) ?: throw RuntimeException("User $id not found")
    return emptySequence()

    val sameSex = sexIndex[account.sex]

    val countryIndex = country?.let { countries[it] }?.let { countryIndex[it] }
    val cityIndex = city?.let { cities[it] }?.let { cityIndex[it] }

    if ((country != null && countryIndex == null)
        || (city != null && cityIndex == null)
    ) {
        return emptySequence()
    }

    if (account.likes==null){
        return emptySequence()
    }
    //val likes = account.interests?.map { interestIndex[it] } ?: return emptySequence()

}
