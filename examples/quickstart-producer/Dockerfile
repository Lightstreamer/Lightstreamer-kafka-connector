# Use the offical Gradle Docker image as builder
FROM gradle AS builder

# Set the working directory
WORKDIR /app

# Copy the project resources
COPY src ./src
COPY build.gradle .
COPY gradlew .
COPY settings.gradle .
COPY gradle.properties .

# Generate the jar file
RUN gradle build

# Use the official Eclipse Temuring image
FROM eclipse-temurin:21-jdk

# Copy the Producer jar from the previuos build stage
COPY --from=builder /app/build/libs/quickstart-producer-all.jar /usr/app/producer.jar

ENTRYPOINT ["java", "-jar", "/usr/app/producer.jar"]
