# ─────────────────────────────────────────────────────────────
# FlowBridge Example App — Multi-Stage Dockerfile
# Uses Spring Boot layered JARs for minimal image rebuild time.
# ─────────────────────────────────────────────────────────────

# ── Stage 1: Build ────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /workspace

# Copy Maven wrapper and parent POM first (cache-friendly)
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

# Copy all submodule POMs
COPY flowbridge-core/pom.xml               flowbridge-core/pom.xml
COPY flowbridge-local/pom.xml              flowbridge-local/pom.xml
COPY flowbridge-embedded/pom.xml           flowbridge-embedded/pom.xml
COPY flowbridge-kafka/pom.xml              flowbridge-kafka/pom.xml
COPY flowbridge-spring-boot-starter/pom.xml flowbridge-spring-boot-starter/pom.xml
COPY flowbridge-dashboard/pom.xml          flowbridge-dashboard/pom.xml
COPY flowbridge-examples/pom.xml           flowbridge-examples/pom.xml

# Resolve dependencies (cached as a separate layer)
RUN ./mvnw dependency:go-offline -B -q

# Copy full source and build
COPY flowbridge-core/src               flowbridge-core/src
COPY flowbridge-local/src              flowbridge-local/src
COPY flowbridge-embedded/src           flowbridge-embedded/src
COPY flowbridge-kafka/src              flowbridge-kafka/src
COPY flowbridge-spring-boot-starter/src flowbridge-spring-boot-starter/src
COPY flowbridge-dashboard/src          flowbridge-dashboard/src
COPY flowbridge-examples/src           flowbridge-examples/src

RUN ./mvnw package -DskipTests -B -q

# Extract layered JAR for minimal final image
WORKDIR /workspace/flowbridge-examples/target
RUN java -Djarmode=layertools -jar flowbridge-examples-*.jar extract

# ── Stage 2: Runtime ──────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

# Add a non-root user for security
RUN addgroup -S flowbridge && adduser -S flowbridge -G flowbridge
USER flowbridge

# Copy layered JAR layers (most-stable layers first for Docker cache efficiency)
COPY --from=builder /workspace/flowbridge-examples/target/dependencies/          ./
COPY --from=builder /workspace/flowbridge-examples/target/spring-boot-loader/    ./
COPY --from=builder /workspace/flowbridge-examples/target/snapshot-dependencies/ ./
COPY --from=builder /workspace/flowbridge-examples/target/application/           ./

# RocksDB data directory (mount as volume for persistence)
VOLUME ["/data/flowbridge/rocksdb"]

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", \
  "--enable-native-access=ALL-UNNAMED", \
  "-XX:+UseVirtualThreads", \
  "org.springframework.boot.loader.launch.JarLauncher"]
