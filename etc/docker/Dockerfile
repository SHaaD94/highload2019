FROM java:8

ADD *.jar app.jar

RUN mkdir -p /opt/shaad/temp/dir/

CMD ["java", \
\
     "-XX:+UseG1GC",\
\
#     "-XX:+PrintGCDetails",\
#     "-XX:+PrintGCApplicationStoppedTime",\
\
     "-Djava.lang.Integer.IntegerCache.high=1300000",\
\
     "-XX:ParallelGCThreads=8",\
     "-Xmx1800m",\
     "-Xms1800m",\
\
     "-jar", "app.jar"]