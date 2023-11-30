FROM maven:latest
COPY src src
COPY pom.xml .
RUN mvn compile
# run exec:java
CMD ["mvn", "exec:java"]