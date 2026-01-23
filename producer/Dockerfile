#------Build stage----------
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app

COPY pom.xml /app/pom.xml
COPY producer/pom.xml /app/producer/pom.xml
COPY consumer/pom.xml /app/consumer/pom.xml
COPY producer/src /app/producer/src

RUN mvn -pl producer -am -DskipTests package

#------Run stage------------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/producer/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]