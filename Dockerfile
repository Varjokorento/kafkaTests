FROM openjdk:8-jre-alpine
WORKDIR /app
EXPOSE 8080
ADD ./build/libs/server.jar .
CMD ["java", "-jar", "server.jar"]