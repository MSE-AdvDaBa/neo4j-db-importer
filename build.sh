#  #!/usr/bin/sh
echo building docker image
docker build ./app -t neo4jtp
echo running containers
docker-compose up
