echo dowloading APOC plugin
mkdir neo4j_mount
mkdir neo4j_mount/plugins
mkdir csv
pushd neo4j_mount/plugins
wget https://github.com/neo4j/apoc/releases/download/5.3.0/apoc-5.3.0-core.jar
popd
echo building docker image
docker build ./app -t neo4jtp
echo running containers
docker-compose up

