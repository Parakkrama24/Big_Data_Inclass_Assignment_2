@echo off
REM Run the Kafka Consumer
cd /d "%~dp0"
set "PATH=C:\Users\parak\apache-maven-3.9.6\bin;%PATH%"
echo Starting Order Consumer...
echo Consumer will process orders and calculate running average
echo Press Ctrl+C to stop
echo.
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.consumer.OrderConsumer"
