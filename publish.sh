gradle clean build shadowJar
cp build/libs/server.jar etc/docker/server.jar
cd etc/docker
docker build . -t stor.highloadcup.ru/accounts/shiny_guest
docker push stor.highloadcup.ru/accounts/shiny_guest