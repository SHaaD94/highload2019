./gradlew clean build shadowJar
cp build/libs/server.jar etc/docker/server.jar
cd etc/docker
docker build . -t stor.highloadcup.ru/accounts/shiny_guest
docker run --rm -v /tmp/data:/tmp/data -p 80:80 -m=2G stor.highloadcup.ru/accounts/shiny_guest