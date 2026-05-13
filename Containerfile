# syntax=docker/dockerfile:1.7
# Containerfile for Kafka Topic Monitor

# Build stage

# NOTE: it would be better to use -alpine variant for a smaller image, but
# Java 17 builds are not available for all architectures. Switch after
# upgrading Java version to 21 or above (here & runtime).
FROM docker.io/library/maven:3-eclipse-temurin-17 AS builder

WORKDIR /build

# Cache dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Build the application (skip tests for faster builds, run tests in CI)
COPY src/ src/
RUN mvn package -DskipTests -B \
    # Verify the JAR was created
    && test -f target/kafka-topic-monitor-*.jar \
    # Rename to predictable name for runtime stage
    && cp target/kafka-topic-monitor-*.jar target/kafka-topic-monitor.jar

# Runtime stage

FROM docker.io/library/eclipse-temurin:17-jre AS runtime

# Labels for container metadata (OCI standard)
LABEL org.opencontainers.image.title="Kafka Topic Monitor" \
      org.opencontainers.image.description="Monitors Kafka consumer lag and reports metrics via metrics4j" \
      org.opencontainers.image.vendor="KairosDB" \
      org.opencontainers.image.source="https://github.com/kairosdb/kafka-monitor" \
      org.opencontainers.image.licenses="Apache-2.0"

# Use non-root user and group for security, use fixed UID/GID for reproducibility
# and Kubernetes security contexts
ARG APP_UID=10001
ARG APP_GID=10001

RUN addgroup --gid ${APP_GID} --system appgroup \
    && adduser --uid ${APP_UID} --system --ingroup appgroup --home /app --shell /sbin/nologin appuser

WORKDIR /app

COPY --from=builder --chown=appuser:appgroup \
    /build/target/kafka-topic-monitor.jar \
    /app/kafka-topic-monitor.jar

COPY --chown=appuser:appgroup conf/ /app/conf/

#RUN apk --no-cache upgrade \
#    && rm -rf /var/cache/apk/* /tmp/* /var/tmp/*
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists /var/cache/apt/*


USER appuser:appgroup

# Environment variables for JVM tuning in containers
# These can be overridden at runtime
ENV JAVA_CONTAINER_OPTS="-XX:+UseContainerSupport \
    -XX:MaxRAMPercentage=75.0 \
    -XX:InitialRAMPercentage=50.0 \
    -XX:+ExitOnOutOfMemoryError"

# Application configuration via environment variables
ENV METRICS4J_CONFIG=/app/conf/metrics4j.conf \
    LOGBACK_CONFIG=/app/conf/logback-container.xml
ENV JAVA_APP_OPTS="-DMETRICS4J_CONFIG=${METRICS4J_CONFIG} \
    -Dlogback.configurationFile=${LOGBACK_CONFIG}"
ENV JAVA_ARGS=""

# Expose default Jooby port
EXPOSE 8080

ENTRYPOINT [ \
    "sh", \
    "-c", \
    "exec java ${JAVA_CONTAINER_OPTS} ${JAVA_APP_OPTS} ${JAVA_ARGS} -jar /app/kafka-topic-monitor.jar $*" \
]
