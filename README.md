# Kafka Order Processing System

A production-ready Apache Kafka-based order processing system demonstrating real-time stream processing, fault tolerance, and enterprise messaging patterns using Apache Avro serialization.

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Running the Application](#running-the-application)
- [Project Structure](#project-structure)
- [Key Implementations](#key-implementations)
- [Monitoring & Debugging](#monitoring--debugging)
- [Documentation](#documentation)
- [Assignment Requirements](#assignment-requirements)
- [License](#license)

## Project Overview

This project implements a complete Kafka-based order processing pipeline for an e-commerce system. It demonstrates:
- Real-time order processing with Avro serialization
- Schema Registry integration for schema evolution
- Fault-tolerant message processing with retry logic
- Dead Letter Queue (DLQ) pattern for failed messages
- Real-time aggregation (running average calculation)
- Production-ready error handling and logging

### Business Use Case
The system simulates an e-commerce platform where:
1. Orders are produced by the `OrderProducer` service
2. Orders are consumed and processed in real-time by `OrderConsumer`
3. Running statistics (average order price) are calculated on-the-fly
4. Failed orders are automatically retried with exponential backoff
5. Permanently failed orders are sent to a Dead Letter Queue for investigation

## Features

### Core Features
- **Apache Avro Serialization**: Type-safe, compact binary message format
- **Schema Registry Integration**: Centralized schema management and version control
- **Real-time Aggregation**: Running average calculation of order prices
- **Retry Logic**: Automatic retry with exponential backoff (max 3 attempts)
- **Dead Letter Queue**: Isolation of permanently failed messages
- **Manual Offset Management**: At-least-once delivery guarantee
- **Idempotent Producer**: Prevents duplicate message production

### Advanced Features
- Simulated failure rate (10%) for demonstration
- Graceful shutdown with cleanup hooks
- Comprehensive logging with SLF4J
- Docker Compose orchestration
- Configurable consumer groups and partitioning

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DOCKER INFRASTRUCTURE                       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────────┐  │
│  │  Zookeeper   │◄───┤    Kafka     │◄───┤ Schema Registry │  │
│  │  (Port 2181) │    │ (Port 9092)  │    │   (Port 8081)   │  │
│  └──────────────┘    └──────┬───────┘    └─────────────────┘  │
└──────────────────────────────┼───────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
         ┌──────────▼──────────┐  ┌──────▼───────────┐
         │  ORDER PRODUCER     │  │  ORDER CONSUMER   │
         │  (OrderProducer)    │  │ (OrderConsumer)   │
         └──────────┬──────────┘  └──────┬───────────┘
                    │                     │
                    └──────────┬──────────┘
                               ▼
                    ┌─────────────────────┐
                    │   KAFKA TOPICS      │
                    │  • orders (main)    │
                    │  • orders-dlq       │
                    └─────────────────────┘
```

For detailed architecture documentation, see [SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md)

## Technologies Used

| Technology | Version | Purpose |
|------------|---------|---------|
| Apache Kafka | 3.6.0 | Distributed message streaming platform |
| Apache Avro | 1.11.3 | Data serialization framework |
| Confluent Schema Registry | 7.5.1 | Schema version management |
| Confluent Kafka Serializers | 7.5.1 | Avro integration for Kafka |
| Java | 11+ | Application runtime |
| Maven | 3.9.6 | Build and dependency management |
| Docker Compose | 3.8 | Infrastructure orchestration |
| SLF4J | 2.0.9 | Logging framework |

## Prerequisites

Before running the project, ensure you have:
- **Java Development Kit (JDK)**: Version 11 or higher
- **Apache Maven**: Version 3.9.6 or higher
- **Docker Desktop**: For running Kafka infrastructure
- **Git**: For version control

## Installation & Setup

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd Inclass_Assignment_2
```

### Step 2: Start Kafka Infrastructure
```bash
docker-compose up -d
```

This will start:
- Zookeeper on port 2181
- Kafka broker on port 9092
- Schema Registry on port 8081

### Step 3: Verify Docker Containers
```bash
docker ps
```

You should see three running containers: `zookeeper`, `kafka`, and `schema-registry`

### Step 4: Compile the Project
```bash
mvn clean compile
```

This will:
- Generate Avro classes from `order.avsc` schema
- Compile Java source code
- Download dependencies

## Running the Application

### Option 1: Using Batch Scripts (Windows - Recommended)

**Terminal 1 - Start Consumer:**
```cmd
run-consumer.bat
```

**Terminal 2 - Start Producer:**
```cmd
run-producer.bat
```

### Option 2: Using Maven Commands

**Terminal 1 - Consumer:**
```bash
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.consumer.OrderConsumer"
```

**Terminal 2 - Producer:**
```bash
mvn exec:java -Dexec.mainClass="com.bigdata.kafka.producer.OrderProducer"
```

### Expected Output

**Producer:**
```
INFO  Starting to produce 10 orders...
INFO  Sent order: OrderID=1001, Product=Laptop, Price=$543.21 -> Partition=0, Offset=0
INFO  Sent order: OrderID=1002, Product=Phone, Price=$289.45 -> Partition=0, Offset=1
```

**Consumer:**
```
INFO  Started consuming from topic: orders
INFO  ✓ Processed Order: ID=1001, Product=Laptop, Price=$543.21 | Running Avg: $543.21 (Total Orders: 1)
INFO  ✓ Processed Order: ID=1002, Product=Phone, Price=$289.45 | Running Avg: $416.33 (Total Orders: 2)
WARN  ⚠ Retry attempt 1/3 for Order ID: 1003
INFO  ✓ Retry successful for Order ID: 1003
```

## Project Structure

```
Inclass_Assignment_2/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/bigdata/kafka/
│       │       ├── producer/
│       │       │   └── OrderProducer.java       # Message producer
│       │       ├── consumer/
│       │       │   └── OrderConsumer.java       # Message consumer with retry & DLQ
│       │       └── avro/
│       │           └── Order.java               # Generated Avro class
│       └── resources/
│           └── avro/
│               └── order.avsc                   # Avro schema definition
├── docker-compose.yml                           # Kafka infrastructure
├── pom.xml                                      # Maven build configuration
├── run-producer.bat                             # Windows batch script for producer
├── run-consumer.bat                             # Windows batch script for consumer
├── setup-maven.ps1                              # PowerShell Maven setup script
├── README.md                                    # This file
├── QUICKSTART.md                                # Quick start guide
├── SYSTEM_ARCHITECTURE.md                       # Detailed architecture documentation
└── DEMONSTRATION_GUIDE.md                       # Demo instructions

```

## Key Implementations

### 1. Avro Schema (`order.avsc`)
```json
{
  "namespace": "com.bigdata.kafka.avro",
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

### 2. Real-Time Aggregation
The consumer calculates running average of order prices:
```java
totalPrice += order.getPrice();
orderCount++;
double runningAverage = totalPrice / orderCount;
```

### 3. Retry Logic with Exponential Backoff
- Attempt 1: Wait 1 second
- Attempt 2: Wait 2 seconds
- Attempt 3: Wait 3 seconds
- After 3 failures: Send to DLQ

### 4. Dead Letter Queue
Failed messages are sent to `orders-dlq` topic with detailed error information.

## Monitoring & Debugging

### View All Kafka Topics
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### View Messages in DLQ
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-dlq --from-beginning
```

### Check Schema Registry
```bash
curl http://localhost:8081/subjects
```

### View Consumer Group Details
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group order-consumer-group
```

### Check Kafka Logs
```bash
docker logs kafka
docker logs schema-registry
```

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)**: Quick start guide with step-by-step instructions
- **[SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md)**: Detailed architecture and design decisions
- **[DEMONSTRATION_GUIDE.md](DEMONSTRATION_GUIDE.md)**: Guide for demonstrating the system

## Assignment Requirements

This project fulfills all Big Data assignment requirements:

- [x] Kafka producer with Avro serialization
- [x] Kafka consumer with Avro deserialization
- [x] Real-time aggregation (running average of order prices)
- [x] Retry logic for temporary failures (max 3 attempts with exponential backoff)
- [x] Dead Letter Queue for permanently failed messages
- [x] Schema Registry integration
- [x] Docker Compose infrastructure setup
- [x] Complete documentation and code comments
- [x] Git version control

## Stopping the System

### Stop Producer and Consumer
Press `Ctrl+C` in both terminal windows

### Stop Docker Containers
```bash
docker-compose down
```

### Clean Up (Optional)
```bash
# Remove all containers and volumes
docker-compose down -v

# Clean Maven build
mvn clean
```

## Troubleshooting

**Kafka not responding:**
```bash
docker-compose restart
docker ps  # Verify all containers are running
```

**Build fails:**
```bash
mvn clean compile
```

**Port already in use:**
```bash
# Stop existing containers
docker-compose down

# Check port usage
netstat -ano | findstr :9092
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Created for Big Data (8th Semester) In-Class Assignment 2

---

For detailed technical documentation, please refer to [SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md)

For a quick start guide, see [QUICKSTART.md](QUICKSTART.md)
