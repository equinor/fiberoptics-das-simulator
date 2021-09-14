FROM azul/zulu-openjdk-alpine:13-jre
LABEL org.opencontainers.image.authors="Espen Tjønneland <etj@knowit.no>"
LABEL org.opencontainers.image.source=https://github.com/equinor/fiberoptics-das-simulator

ARG HTTP_PROXY
ARG http_proxy
ARG HTTPS_PROXY
ARG https_proxy
ARG JAR_FILE

COPY ${JAR_FILE} /opt/app.jar

VOLUME /tmp

RUN adduser -D -s /sbin/nologin ingressuser
WORKDIR /opt
USER ingressuser
ENTRYPOINT ["java", "-XshowSettings:vm", "-XX:+PrintFlagsFinal", "-XX:MaxRAMPercentage=70", "-Djava.security.egd=file:/dev/./urandom", "-Djava.awt.headless=true", "-jar", "/opt/app.jar"]
