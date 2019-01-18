package com.shaad.highload2018.repository

import com.google.common.primitives.UnsignedBytes
import com.shaad.highload2018.domain.InnerAccount
import com.shaad.highload2018.utils.*
import com.shaad.highload2018.web.get.FilterRequest

private val lexComparator = UnsignedBytes.lexicographicalComparator()
fun filter(filterRequest: FilterRequest): Sequence<InnerAccount> {
    val indexes = mutableListOf<IntIterator>()
    filterRequest.email?.let { (domain, _, _) ->
        if (domain != null) indexes.add(emailDomainIndex[domain].getPartitionedIterator())
    }
    filterRequest.sex?.let { (eq) ->
        indexes.add(sexIndex[sex2Int(eq)].getPartitionedIterator())
    }
    filterRequest.premium?.let { (now, _) ->
        if (now != null) {
            indexes.add(premiumNowIndex.getPartitionedIterator())
        }
    }
    filterRequest.status?.let { (eq, neq) ->
        if (eq != null) {
            indexes.add(statusIndex[statuses[eq]!!].getPartitionedIterator())
        }
        if (neq != null) {
            statuses.keys().asSequence().filter { it != neq }.map { statuses[it]!! }
                .map { statusIndex[it].getPartitionedIterator() }
                .toList()
                .let { indexes.add(joinIterators(it)) }
        }
    }

    filterRequest.fname?.let { (eq, any, _) ->
        if (eq != null) indexes.add(fnames[eq]?.let { fnameIndex[it] }.getPartitionedIterator())
        if (any != null) indexes.add(any.map {
            fnames[it]?.let { fnameIndex[it] }.getPartitionedIterator()
        }.let { joinIterators(it) })
    }

    filterRequest.sname?.let { (eq, starts, _) ->
        if (starts != null) {
            indexes.add(snames.filter { it.key.startsWith(starts) }.map {
                snameIndex[it.value].getPartitionedIterator()
            }.let { joinIterators(it) })
        }
        if (eq != null) indexes.add(snames[eq]?.let { snameIndex[it] }.getPartitionedIterator())
    }

    filterRequest.phone?.let { (eq, _) ->
        if (eq != null) indexes.add(phoneCodeIndex[eq.toInt()].getPartitionedIterator())
    }

    filterRequest.country?.let { (eq, _) ->
        if (eq != null) indexes.add(countries[eq]?.let { countryIndex[it] }.getPartitionedIterator())
    }

    filterRequest.city?.let { (eq, any, _) ->
        if (eq != null) indexes.add(cities[eq]?.let { cityIndex[it] }.getPartitionedIterator())
        if (any != null) indexes.add(any.map {
            cities[it]?.let { cityIndex[it] }.getPartitionedIterator()
        }.let { joinIterators(it) })
    }

    filterRequest.birth?.let { (lt, gt, year) ->
        if (lt != null || gt != null) {
            val ltY = checkBirthYear(lt?.let { getYear(lt) - 1920 } ?: 99)
            val gtY = checkBirthYear(gt?.let { getYear(gt) - 1920 } ?: 0)

            val iterators = (gtY..ltY).asSequence()
                .map { birthIndex[it] }
                .map { it.getPartitionedIterator() }
                .toMutableList()

            indexes.add(joinIterators(iterators))
        }

        if (year != null) indexes.add(birthIndex[checkBirthYear(year - 1920)].getPartitionedIterator())
    }

    filterRequest.interests?.let { (contains, any) ->
        contains?.let { containsInterests ->
            containsInterests.asSequence()
                .map { interests[it]?.let { interestIndex[it] }.getPartitionedIterator() }
                .forEach { indexes.add(it) }
        }
        any?.let { anyInterests ->
            anyInterests.map { interests[it]?.let { interestIndex[it] }.getPartitionedIterator() }
        }?.let { indexes.add(joinIterators(it)) }
    }

    filterRequest.likes?.let { (contains) ->
        contains?.let { likes ->
            likes.asSequence().map { getLikesByIndex(it)?.intIterator() ?: emptyIntIterator() }
                .forEach { indexes.add(it) }
        }
    }

    val iterator = if (indexes.isEmpty()) fullIdsIterator() else generateIteratorFromIndexes(indexes)

    val result = ArrayList<InnerAccount>(filterRequest.limit)
    while (iterator.hasNext() && result.size != filterRequest.limit) {
        val acc = getAccountByIndex(iterator.nextInt()) ?: continue

        val isFilteredByEmail = if (filterRequest.email != null) {
            val ltFilter = if (filterRequest.email.lt != null) {
                lexComparator.compare(acc.email, filterRequest.email.lt) <= 0
            } else true

            val gtFilter = if (filterRequest.email.gt != null) {
                lexComparator.compare(acc.email, filterRequest.email.gt) >= 0
            } else true
            ltFilter && gtFilter
        } else true

        if (!isFilteredByEmail) {
            continue
        }

        val isFilteredByNull = filterByNull(filterRequest.fname?.nill, acc.fname) &&
                filterByNull(filterRequest.sname?.nill, acc.sname) &&
                filterByNull(filterRequest.phone?.nill, acc.phone) &&
                filterByNull(filterRequest.country?.nill, acc.country) &&
                filterByNull(filterRequest.city?.nill, acc.city) &&
                filterByNull(filterRequest.premium?.nill, acc.premium)

        if (!isFilteredByNull) {
            continue
        }

        val isFilteredByAge = if (filterRequest.birth != null) {
            val ltFilter = if (filterRequest.birth.lt != null) {
                acc.birth <= filterRequest.birth.lt
            } else true

            val gtFilter = if (filterRequest.birth.gt != null) {
                acc.birth >= filterRequest.birth.gt
            } else true
            ltFilter && gtFilter
        } else true

        if (!isFilteredByAge) {
            continue
        }
        result.add(acc)
    }
    return result.asSequence()
}
