FROM eclipse-temurin:21-jre-jammy
LABEL org.opencontainers.image.authors="Espen Tjønneland <etj@knowit.no>"
LABEL org.opencontainers.image.source=https://github.com/equinor/fiberoptics-das-simulator

VOLUME /tmp

RUN useradd -u 1000 ingressuser

WORKDIR /opt
USER ingressuser

COPY --from=builder /target/*.jar /opt/app.jar

ENTRYPOINT ["java", "-XshowSettings:vm", "-XX:+PrintFlagsFinal", "-XX:MaxRAMPercentage=70", "-Djava.security.egd=file:/dev/./urandom", "-Djava.awt.headless=true", "-jar", "/opt/app.jar"]
