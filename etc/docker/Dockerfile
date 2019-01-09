FROM frolvlad/alpine-oraclejdk8:slim

ADD *.jar app.jar

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
#     "-XX:+PrintGCDetails",\
#     "-XX:+PrintGCApplicationStoppedTime",\
\
     "-Djava.lang.Integer.IntegerCache.high=1300000",\
\
     "-XX:ParallelGCThreads=4",\
     "-XX:InitiatingHeapOccupancyPercent=70", \
     "-XX:MaxGCPauseMillis=100",\
     "-XX:G1ReservePercent=5",\
\
     "-Xmx1700m",\
     "-Xms1700m",\
\
     "-jar", "app.jar"]