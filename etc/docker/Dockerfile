FROM frolvlad/alpine-oraclejdk8:slim

EXPOSE 80

ADD *.jar app.jar

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
#     "-XX:+PrintGCDetails",\
     "-XX:+ShowMessageBoxOnError",\
#     "-XX:+PrintGCApplicationStoppedTime",\
#     "-XX:+PrintGCTimeStamps",\
\
     "-Djava.lang.Integer.IntegerCache.high=1700000",\
\
     "-XX:ParallelGCThreads=2",\
#     "-XX:InitiatingHeapOccupancyPercent=40", \
     "-XX:MaxGCPauseMillis=500",\
     "-XX:G1ReservePercent=5",\
\
#     "-XX:+OptimizeStringConcat",\
#     "-XX:+EliminateLocks",\
#     "-XX:+UseCompressedOops",\
\
\
     "-XX:MaxMetaspaceSize=64m",\
#     "-XX:SurvivorRatio=6",\
\
     "-XX:MaxNewSize=300m",\
     "-XX:NewSize=300m",\
#     "-XX:-UseAdaptiveSizePolicy",\
\
     "-Xmx1300m",\
     "-Xms1300m",\
\
     "-jar", "app.jar"]
