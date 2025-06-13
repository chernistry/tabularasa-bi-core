FROM openjdk:11-jdk-slim

RUN apt-get update && apt-get install -y maven git && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN cd root/q1_realtime_stream_processing && mvn clean test

CMD ["bash"] 