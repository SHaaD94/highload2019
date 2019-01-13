FROM openjdk:latest

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
     "-XX:MaxNewSize=100m",\
     "-XX:NewSize=100m",\
#     "-XX:-UseAdaptiveSizePolicy",\
\
     "-Xmx1100m",\
     "-Xms1100m",\
\
     "-jar", "app.jar"]
