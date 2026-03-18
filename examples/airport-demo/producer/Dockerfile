# Use the official Eclipse Temurin image  
FROM eclipse-temurin:21-jdk

# Copy the Producer jar produced by the Gradle build
COPY build/libs/example-kafka-connector-demo-publisher-*-all.jar /usr/app/producer.jar

ENTRYPOINT ["java", "-jar", "/usr/app/producer.jar"]
