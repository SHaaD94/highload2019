FROM frolvlad/alpine-oraclejdk8:slim

EXPOSE 80

ADD *.jar app.jar

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
     "-XX:+PrintGCDetails",\
#     "-XX:+ShowMessageBoxOnError",\
     "-XX:+PrintGCApplicationStoppedTime",\
#     "-XX:+PrintGCTimeStamps",\
#     "-XX:MaxDirectMemorySize=200m",\
     "-XX:CompileThreshold=100",\
     "-XX:Tier2CompileThreshold=100",\
     "-XX:Tier3CompileThreshold=500",\
     "-XX:Tier4CompileThreshold=1000",\
\
     "-XX:ParallelGCThreads=2",\
#     "-XX:InitiatingHeapOccupancyPercent=40", \
     "-XX:MaxGCPauseMillis=500",\
     "-XX:G1ReservePercent=5",\
\
#     "-XX:+OptimizeStringConcat",\
#     "-XX:+EliminateLocks",\
     "-XX:+UseCompressedOops",\
     "-XX:+UseCompressedClassPointers",\
     "-XX:+AggressiveOpts",\
     "-XX:AutoBoxCacheMax=20000",\
#     "-XX:CompileThreshold=10",\
\
\
     "-XX:MaxMetaspaceSize=64m",\
#     "-XX:SurvivorRatio=6",\
\
     "-XX:MaxNewSize=500m",\
     "-XX:NewSize=500m",\
#     "-XX:-UseAdaptiveSizePolicy",\
\
     "-Xmx1800m",\
     "-Xms1800m",\
     "-Xss228k",\
\
     "-jar", "app.jar"]
