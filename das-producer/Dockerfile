FROM eclipse-temurin:21-jre-jammy
LABEL org.opencontainers.image.authors="Espen Tjønneland <etj@knowit.no>"
LABEL org.opencontainers.image.source=https://github.com/equinor/fiberoptics-das-simulator

VOLUME /tmp

RUN useradd -u 1000 ingressuser

WORKDIR /opt
USER ingressuser

ENV JAR_FILE=das-producer/target/*.jar
COPY ${JAR_FILE} /opt/app.jar

ENTRYPOINT ["java", "-XshowSettings:vm", "-XX:+PrintFlagsFinal", "-XX:MaxRAMPercentage=70", "-Djava.security.egd=file:/dev/./urandom", "-Djava.awt.headless=true", "-jar", "/opt/app.jar"]
