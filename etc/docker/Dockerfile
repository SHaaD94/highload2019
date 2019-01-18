FROM frolvlad/alpine-oraclejdk8:slim

EXPOSE 80

ADD *.jar app.jar

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
#     "-XX:+PrintGCDetails",\
#     "-XX:+ShowMessageBoxOnError",\
#     "-XX:+PrintGCApplicationStoppedTime",\
#     "-XX:+PrintGCTimeStamps",\
     "-XX:+UnlockDiagnosticVMOptions",\
     "-XX:NativeMemoryTracking=summary",\
     "-XX:+PrintNMTStatistics",\
\
     "-XX:ParallelGCThreads=2",\
#     "-XX:InitiatingHeapOccupancyPercent=40", \
     "-XX:MaxGCPauseMillis=500",\
     "-XX:G1ReservePercent=1",\
\
#     "-XX:+OptimizeStringConcat",\
#     "-XX:+EliminateLocks",\
     "-XX:+UseCompressedOops",\
     "-XX:+AggressiveOpts",\
     "-XX:AutoBoxCacheMax=20000",\
#     "-XX:CompileThreshold=10",\
\
\
     "-XX:MaxMetaspaceSize=64m",\
#     "-XX:SurvivorRatio=6",\
\
     "-XX:MaxNewSize=200m",\
     "-XX:NewSize=200m",\
#     "-XX:-UseAdaptiveSizePolicy",\
\
     "-Xmx1600m",\
     "-Xms1600m",\
     "-Xss228k",\
\
     "-jar", "app.jar"]
