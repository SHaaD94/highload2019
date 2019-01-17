package com.shaad.highload2018.utils

import org.agrona.collections.IntArrayList


object EmptyIterator : Iterator<Nothing> {
    override fun hasNext() = false
    override fun next(): Nothing {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

object EmptyIntIterator : IntIterator() {
    override fun hasNext() = false
    override fun nextInt(): Int {
        TODO("not implemented")
    }
}


fun generateIteratorFromIndexes(indexes: List<IntIterator>): IntIterator =
    if (indexes.isEmpty()) emptyIntIterator() else
        object : IntIterator() {
            private val currentVal = IntArray(indexes.size) { IntArrayList.DEFAULT_NULL_VALUE }
            private var next = IntArrayList.DEFAULT_NULL_VALUE

            init {
                var i = 0
                while (i < indexes.size) {
                    if (indexes[i].hasNext()) {
                        currentVal[i] = indexes[i].nextInt()
                        i++
                    } else {
                        break
                    }
                }
                findNext()
            }

            override fun hasNext() = next != IntArrayList.DEFAULT_NULL_VALUE

            override fun nextInt(): Int {
                val next = this.next
                findNext()
                return next
            }

            private fun findNext() {
                next = IntArrayList.DEFAULT_NULL_VALUE
                var newNext = IntArrayList.DEFAULT_NULL_VALUE
                while (newNext == IntArrayList.DEFAULT_NULL_VALUE) {
                    var allEqual = true
                    var id2Yield = -1
                    var i = 0
                    while (i < indexes.size) {
                        val c = currentVal[i]
                        if (id2Yield == -1) {
                            id2Yield = c
                        } else {
                            if (id2Yield != c) {
                                allEqual = false
                            }
                            if (id2Yield > c) {
                                id2Yield = c
                            }
                        }
                        i++
                    }
                    if (allEqual) {
                        newNext = id2Yield
                        i = 0
                        while (i < indexes.size) {
                            if (!indexes[i].hasNext()) {
                                next = IntArrayList.DEFAULT_NULL_VALUE
                                return
                            }
                            currentVal[i] = indexes[i].nextInt()
                            i++
                        }
                    } else {
                        i = 0
                        while (i < indexes.size) {
                            if (currentVal[i] >= id2Yield) {
                                while (currentVal[i] > id2Yield) {
                                    if (!indexes[i].hasNext()) {
                                        next = IntArrayList.DEFAULT_NULL_VALUE
                                        return
                                    }
                                    currentVal[i] = indexes[i].nextInt()
                                }
                            }
                            i++
                        }
                    }
                }
                next = newNext
            }
        }

fun Array<IntArrayList>?.getPartitionedIterator(): IntIterator {
    this ?: return emptyIntIterator()
    return object : IntIterator() {
        private var lists = this@getPartitionedIterator!!
        private var next = Int.MIN_VALUE
        private var curNum = 0
        private var curList = this@getPartitionedIterator.size - 1

        init {
            if (!lists.isEmpty()) {
                findNext()
            }
        }

        override fun hasNext() = next != Int.MIN_VALUE

        override fun nextInt(): Int {
            val num = next
            findNext()
            return num
        }

        private fun findNext() {
            while (true) {
                if ((lists[curList].isEmpty() || curNum == lists[curList].size)) {
                    if (curList == 0) {
                        next = Int.MIN_VALUE
                        return
                    }
                    curNum = 0
                    curList--
                    continue
                }
                next = lists[curList][curNum]
                curNum++
                return
            }

        }
    }
}

fun emptyIterator() = EmptyIterator
fun emptyIntIterator() = EmptyIntIterator

fun joinIterators(indexes: List<IntIterator>): IntIterator =
    if (indexes.isEmpty()) emptyIntIterator() else
        object : IntIterator() {
            private val currentVal = IntArray(indexes.size) { IntArrayList.DEFAULT_NULL_VALUE }
            private var next = IntArrayList.DEFAULT_NULL_VALUE

            init {
                var i = 0
                while (i < indexes.size) {
                    if (indexes[i].hasNext()) {
                        currentVal[i] = indexes[i].nextInt()
                    }
                    i++
                }
                findNext()
            }

            override fun hasNext() = next !=IntArrayList.DEFAULT_NULL_VALUE

            override fun nextInt(): Int {
                val next = this.next
                findNext()
                return next
            }

            private fun findNext() {
                var nextNext = IntArrayList.DEFAULT_NULL_VALUE
                while (nextNext == IntArrayList.DEFAULT_NULL_VALUE) {
                    var maxValue: Int = IntArrayList.DEFAULT_NULL_VALUE
                    var j = 0
                    while (j < currentVal.size) {
                        if (currentVal[j] == IntArrayList.DEFAULT_NULL_VALUE) {
                            j++
                            continue
                        }
                        val it = currentVal[j]
//                        if (maxValue == IntArrayList.DEFAULT_NULL_VALUE) {
//                            maxValue = it
//                        }
                        if (maxValue < it) {
                            maxValue = it
                        }
                        j++
                    }
                    if (maxValue == IntArrayList.DEFAULT_NULL_VALUE) {
                        next = IntArrayList.DEFAULT_NULL_VALUE
                        return
                    }
                    nextNext = maxValue
                    var i=0
                    //many with maxvalue possible
                    while(i<currentVal.size) {
                        if (currentVal[i]==maxValue) {
                            if (currentVal[i] != IntArrayList.DEFAULT_NULL_VALUE) {
                                currentVal[i] = if (!indexes[i].hasNext()) {
                                    IntArrayList.DEFAULT_NULL_VALUE
                                } else {
                                    indexes[i].nextInt()
                                }
                            }
                        }
                    }
                }
                next = nextNext
            }
        }

fun IntArrayList.intIterator(): IntIterator = object : IntIterator() {
    private var index = 0
    override fun hasNext() = index < this@intIterator.size
    override fun nextInt() = try {
        this@intIterator[index++]
    } catch (e: ArrayIndexOutOfBoundsException) {
        index -= 1; throw NoSuchElementException(e.message)
    }
}

fun IntArray.intIterator(): IntIterator = object : IntIterator() {
    private var index = 0
    override fun hasNext() = index < this@intIterator.size
    override fun nextInt() = try {
        this@intIterator[index++]
    } catch (e: ArrayIndexOutOfBoundsException) {
        index -= 1; throw NoSuchElementException(e.message)
    }
}