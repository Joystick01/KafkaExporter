FROM maven:3.9-eclipse-temurin-23 as build

WORKDIR /build
COPY . .

RUN mvn clean package

FROM eclipse-temurin:23-jre as runtime
LABEL org.opencontainers.image.source=https://github.com/Joystick01/KafkaExporter

WORKDIR /app
COPY --from=build /build/target/*with-dependencies.jar app.jar
CMD ["java", "-jar", "-Xms512m", "-Xmx7G" ,"app.jar"]