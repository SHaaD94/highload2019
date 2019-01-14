./gradlew clean build shadowJar
cp build/libs/server.jar etc/docker/server.jar
cd etc/docker
sudo docker build . -t stor.highloadcup.ru/accounts/shiny_guest
sudo docker run --rm -v /tmp/data:/tmp/data -p 80:80 -m=1600m --memory-swap=0 stor.highloadcup.ru/accounts/shiny_guest