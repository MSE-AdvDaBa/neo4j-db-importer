#  #!/usr/bin/sh
# wget if not already present
if [ ! -f /json/sanitized.json ]; then
    echo "File not found!"
    if [ ! -f /json/dblpv13.json ]; then
        echo "File not found!"
        wget 160.98.47.134/dblpv13.json -O /json/dblpv13.json
    fi
    echo Parsing input file
    sed -E 's/(NumberInt)\(([0-9]+)(\))/\2/' /json/dblpv13.json > /json/sanitized.json
    rm /json/dblpv13.json
fi
mvn exec:java
