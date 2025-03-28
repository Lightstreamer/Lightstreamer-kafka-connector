# Use the official Gradle Docker image as builder
FROM gradle AS builder

# Set the working directory
WORKDIR /app

# Copy the project resources
COPY gradle ./gradle
COPY src ./src
COPY build.gradle .
COPY gradlew .
COPY settings.gradle .

# Generate the jar
RUN gradle build --no-daemon

# Use the official Eclipse Temurin image  
FROM eclipse-temurin:21-jdk

# Copy the Producer jar from the previous build stage
COPY --from=builder /app/build/libs/example-kafka-connector-demo-publisher-all-1.0.0.jar /usr/app/producer.jar

ENTRYPOINT ["java", "-jar", "/usr/app/producer.jar"]
