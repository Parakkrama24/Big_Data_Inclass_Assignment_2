# Quick Start Guide

## System Status

All components are ready to run!

- ✅ Maven 3.9.6 installed
- ✅ Project compiled successfully
- ✅ Kafka, Zookeeper, and Schema Registry running in Docker
- ✅ Producer and Consumer code ready

## How to Run the System

### Option 1: Using Batch Scripts (Easiest)

**Step 1: Start the Consumer** (in a new terminal)
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
run-consumer.bat
```

**Step 2: Start the Producer** (in another terminal)
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
run-producer.bat
```

### Option 2: Using Maven Commands

**Set Maven in PATH first:**
```cmd
set PATH=C:\Users\parak\apache-maven-3.9.6\bin;%PATH%
```

**Terminal 1 - Consumer:**
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.consumer.OrderConsumer"
```

**Terminal 2 - Producer:**
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.producer.OrderProducer"
```

## What to Expect

### Consumer Output:
```
INFO  Started consuming from topic: orders
INFO  DLQ Topic: orders-dlq
INFO  Max Retry Attempts: 3
========================================

INFO  ✓ Processed Order: ID=1001, Product=Laptop, Price=$543.21 | Running Avg: $543.21 (Total Orders: 1)
INFO  ✓ Processed Order: ID=1002, Product=Phone, Price=$289.45 | Running Avg: $416.33 (Total Orders: 2)
WARN  ⚠ Retry attempt 1/3 for Order ID: 1003
INFO  ✓ Retry successful for Order ID: 1003
...
```

### Producer Output:
```
INFO  Starting to produce 10 orders...
INFO  Sent order: OrderID=1001, Product=Laptop, Price=$543.21 -> Partition=0, Offset=0
INFO  Sent order: OrderID=1002, Product=Phone, Price=$289.45 -> Partition=0, Offset=1
...
```

## Key Features Demonstrated

1. **Avro Serialization**: All messages use Avro schema with Schema Registry
2. **Real-time Aggregation**: Running average of prices calculated in real-time
3. **Retry Logic**: Failed messages retried up to 3 times with exponential backoff
4. **Dead Letter Queue**: Permanently failed messages sent to DLQ topic
5. **Simulated Failures**: 10% failure rate for demonstration purposes

## Monitoring Commands

**View all Kafka topics:**
```cmd
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**View DLQ messages:**
```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-dlq --from-beginning
```

**Check Schema Registry:**
```cmd
curl http://localhost:8081/subjects
```

## Stopping the System

1. Press `Ctrl+C` in both terminal windows to stop Producer and Consumer
2. Stop Docker containers:
   ```cmd
   docker-compose down
   ```

## Troubleshooting

**If Kafka isn't responding:**
```cmd
docker-compose restart
docker ps  # Verify all containers are running
```

**If build fails:**
```cmd
mvn clean compile
```

## Assignment Completion Checklist

- [x] Kafka producer with Avro serialization
- [x] Kafka consumer with Avro deserialization
- [x] Real-time aggregation (running average of prices)
- [x] Retry logic for temporary failures (max 3 attempts)
- [x] Dead Letter Queue for permanently failed messages
- [x] Schema Registry integration
- [x] Docker Compose for infrastructure
- [x] Complete documentation
- [x] Git repository ready for submission

## Next Steps for Submission

1. Initialize Git repository:
   ```cmd
   git init
   git add .
   git commit -m "Complete Kafka Order Processing System with Avro serialization"
   ```

2. Push to GitHub/GitLab

3. Demonstrate the system running live

Good luck with your assignment! 🚀
