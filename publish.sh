./gradlew clean build shadowJar
cp build/libs/server.jar etc/docker/server.jar
cd etc/docker
sudo docker build . -t stor.highloadcup.ru/accounts/shiny_guest
sudo docker push stor.highloadcup.ru/accounts/shiny_guest