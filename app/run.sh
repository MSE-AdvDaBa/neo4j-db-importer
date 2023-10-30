#  #!/usr/bin/sh
echo parsing input file
# sed -E 's/(NumberInt)\(([0-9]+)(\))/\2/' /dblpExample.json > /json/sanitized.json
# sed -E 's/(NumberInt)\(([0-9]+)(\))/\2/' /dblpv13.json > /json/sanitized.json
mvn exec:java
