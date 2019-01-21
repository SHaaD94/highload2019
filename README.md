# highload2019

## Status
Aborted competition - docker container didn't make it through OOM, even with stable 300 MB free RAM left.

## Stats:
- Time spent : 100+ hours
- Kotlin *gotcha*s found : 15+
- Critical Kotlin *gotcha*s found (nearly switched back to Java) : 3+
- Learned new JVM flags : 50+
- Custom iterators written : 7
- Stable JVM crashes(passing Java Integer to Kotlin Int) : 1

## Achievements:
- 6.5k *GET* RPS on 8 cores
- Handmade indexes, with possible merging. (Partitioned arrays of int arrays with ids sorted by descending)
- Learned a lot about memory model, optimizations and profiling
- Total used memory after loading all data in *RAM* with indexes: *1201 MB*

## Doubtful achievements
- Tried procedural programming
- Tried off-heap collections
- Leaned more about bit operations
- Manual to byte JSON serialization 
- Manual byte handling 
- Tried Epoll(0) 
- Tried java 11 
- Tried gradle kotlin dsl 

##
- Will ever use 'ranges': no
