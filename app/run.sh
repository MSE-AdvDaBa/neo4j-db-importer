#  #!/usr/bin/sh
echo parsing input file
sed -E 's/(NumberInt)\(([0-9]+)(\))/\2/' /dblpv13.json > /chungus.json
mvn exec:java
