@echo off
REM Run the Kafka Producer
cd /d "%~dp0"
set "PATH=C:\Users\parak\apache-maven-3.9.6\bin;%PATH%"
echo Starting Order Producer...
echo.
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.producer.OrderProducer" -Dexec.args="10"
