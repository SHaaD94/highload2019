FROM java:8

ADD *.jar app.jar

CMD ["java", "-XX:+UseG1GC", "-XX:ParallelGCThreads=8", "-XX:MaxGCPauseMillis=500", "-Xmx1800m", "-Xms1800m", "-jar", "app.jar"]