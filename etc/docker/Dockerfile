FROM frolvlad/alpine-oraclejdk8:slim

EXPOSE 80

ADD *.jar app.jar

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
     "-XX:+PrintGCDetails",\
#     "-XX:+PrintGCApplicationStoppedTime",\
\
     "-Djava.lang.Integer.IntegerCache.high=1700000",\
\
     "-XX:ParallelGCThreads=4",\
     "-XX:InitiatingHeapOccupancyPercent=40", \
     "-XX:MaxGCPauseMillis=500",\
     "-XX:G1ReservePercent=5",\
\
     "-XX:+OptimizeStringConcat",\
     "-XX:+EliminateLocks",\
     "-XX:+UseCompressedOops",\
\
\
     "-XX:MaxMetaspaceSize=128m",\
     "-XX:SurvivorRatio=6",\
\
     "-XX:MaxNewSize=300m",\
     "-XX:NewSize=300m",\
     "-XX:-UseAdaptiveSizePolicy",\
\
     "-Xmx1500m",\
     "-Xms1500m",\
\
     "-jar", "app.jar"]
