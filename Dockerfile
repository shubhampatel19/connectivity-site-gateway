# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -ntp -q dependency:go-offline
COPY src ./src
RUN mvn -B -ntp -q package -DskipTests

FROM eclipse-temurin:21-jre-noble
RUN useradd --system --uid 10001 --home-dir /var/lib/connectivity-gateway gateway \
 && mkdir -p /var/lib/connectivity-gateway \
 && chown gateway /var/lib/connectivity-gateway
COPY --from=build /src/target/site-gateway.jar /app/site-gateway.jar
USER gateway
# A small fixed heap makes memory problems show up quickly in drills.
# ExitOnOutOfMemoryError: a JVM that has hit OOM is in an unknown state;
# better to die and be restarted than to limp along.
ENV JAVA_OPTS="-Xms64m -Xmx256m -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"
EXPOSE 8090
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/site-gateway.jar"]
