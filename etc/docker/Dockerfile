FROM frolvlad/alpine-oraclejdk8:slim

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
     "-XX:MaxGCPauseMillis=300",\
     "-XX:G1ReservePercent=5",\
\
     "-XX:MaxMetaspaceSize=128m",\
\
     "-Xmx1400m",\
     "-Xms1400m",\
\
     "-jar", "app.jar"]