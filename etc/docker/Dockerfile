FROM java:8

ADD *.jar app.jar

CMD ["java", "XX:+UseG1GC", "-XX:ParallelGCThreads=4", "-XX:MaxGCPauseMillis=100", "-Xmx1800m", "-Xms1800m", "-jar", "app.jar"]