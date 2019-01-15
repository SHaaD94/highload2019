FROM frolvlad/alpine-oraclejdk8:slim

EXPOSE 80

ADD *.jar app.jar

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
     "-XX:+PrintGCDetails",\
     "-XX:+ShowMessageBoxOnError",\
     "-XX:+PrintGCApplicationStoppedTime",\
     "-XX:+PrintGCTimeStamps",\
\
     "-Djava.lang.Integer.IntegerCache.high=1700000",\
\
     "-XX:ParallelGCThreads=4",\
#     "-XX:InitiatingHeapOccupancyPercent=40", \
     "-XX:MaxGCPauseMillis=500",\
     "-XX:G1ReservePercent=5",\
\
#     "-XX:+OptimizeStringConcat",\
#     "-XX:+EliminateLocks",\
     "-XX:+UseCompressedOops",\
     "-XX:CompileThreshold=10",\
\
\
     "-XX:MaxMetaspaceSize=64m",\
#     "-XX:SurvivorRatio=6",\
\
     "-XX:MaxNewSize=400m",\
     "-XX:NewSize=400m",\
#     "-XX:-UseAdaptiveSizePolicy",\
\
     "-Xmx1700m",\
     "-Xms1700m",\
\
     "-jar", "app.jar"]
