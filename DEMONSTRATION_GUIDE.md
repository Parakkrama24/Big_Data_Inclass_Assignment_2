# Kafka Order Processing System - Demonstration Guide

## Table of Contents
1. [Pre-Demonstration Checklist](#pre-demonstration-checklist)
2. [Step-by-Step Demonstration](#step-by-step-demonstration)
3. [What to Explain During Demo](#what-to-explain-during-demo)
4. [Expected Output Examples](#expected-output-examples)
5. [Common Questions & Answers](#common-questions--answers)
6. [Troubleshooting During Demo](#troubleshooting-during-demo)

---

## Pre-Demonstration Checklist

### Before Starting the Demo

- [ ] **Docker is running** and all containers are up
- [ ] **Maven is configured** in PATH
- [ ] **Project is compiled** (BUILD SUCCESS)
- [ ] **Two terminal windows** are ready
- [ ] **Code editor** open with key files
- [ ] **Network connection** is stable (for Maven dependencies)

### Quick Verification Commands

```cmd
# Check Docker containers
docker ps

# Should show:
# - zookeeper (Up 2 minutes)
# - kafka (Up 2 minutes)
# - schema-registry (Up 2 minutes)

# Verify Maven
set PATH=C:\Users\parak\apache-maven-3.9.6\bin;%PATH%
mvn -version

# Should show Maven 3.9.6, Java 21
```

---

## Step-by-Step Demonstration

### Part 1: System Setup (2-3 minutes)

#### Step 1.1: Show Project Structure
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
dir
```

**What to point out:**
```
📁 Inclass_Assignment_2/
├── 📄 docker-compose.yml          ← Kafka infrastructure
├── 📄 pom.xml                     ← Maven dependencies
├── 📁 src/main/
│   ├── 📁 java/
│   │   └── 📁 com/bigdata/kafka/
│   │       ├── 📁 producer/
│   │       │   └── OrderProducer.java
│   │       └── 📁 consumer/
│   │           └── OrderConsumer.java
│   └── 📁 resources/
│       └── 📁 avro/
│           └── order.avsc         ← Avro schema
├── 📄 run-producer.bat
├── 📄 run-consumer.bat
└── 📄 README.md
```

**Talking points:**
> "This is a Maven-based Java project that implements a Kafka order processing system with Avro serialization."

#### Step 1.2: Show Avro Schema
```cmd
type src\main\resources\avro\order.avsc
```

**Explain:**
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

**Talking points:**
> "This Avro schema defines our order message structure. Maven will generate a Java class from this schema during compilation. Avro provides type safety and efficient binary serialization."

#### Step 1.3: Verify Docker Infrastructure
```cmd
docker-compose ps
```

**Explain each service:**
- **Zookeeper** (Port 2181): Manages Kafka cluster coordination
- **Kafka** (Port 9092): Message broker that stores and serves messages
- **Schema Registry** (Port 8081): Centralized schema management for Avro

**Talking points:**
> "We're using Confluent's Kafka distribution with Schema Registry for managing our Avro schemas. All services are running in Docker containers for easy setup."

---

### Part 2: Code Walkthrough (3-5 minutes)

#### Step 2.1: Show Producer Code (OrderProducer.java)

**Open in editor** and highlight key sections:

**1. Producer Configuration:**
```java
// Lines 30-40
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          KafkaAvroSerializer.class.getName());
props.put("schema.registry.url", "http://localhost:8081");
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

**Talking points:**
> "The producer uses KafkaAvroSerializer to automatically serialize our Order objects. We're using `acks=all` for maximum durability and idempotence for exactly-once semantics."

**2. Message Creation:**
```java
// Lines 56-60
Order order = Order.newBuilder()
    .setOrderId(String.valueOf(1000 + i))
    .setProduct(products[random.nextInt(products.length)])
    .setPrice(random.nextFloat() * 1000 + 10)
    .build();
```

**Talking points:**
> "We're using the Avro-generated Order class with a builder pattern. The producer generates random orders with products like Laptop, Phone, Tablet, etc., with prices between $10 and $1010."

#### Step 2.2: Show Consumer Code (OrderConsumer.java)

**Open in editor** and highlight key sections:

**1. Consumer Configuration:**
```java
// Lines 56-66
props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          KafkaAvroDeserializer.class.getName());
props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
```

**Talking points:**
> "The consumer uses KafkaAvroDeserializer and disables auto-commit for manual offset management, ensuring at-least-once delivery."

**2. Real-Time Aggregation:**
```java
// Lines 85-89
orderCount++;
totalPrice += order.getPrice();
double runningAverage = totalPrice / orderCount;

logger.info("✓ Processed Order: ID={}, Product={}, Price=${} | Running Avg: ${} (Total: {})",
    order.getOrderId(), order.getProduct(),
    String.format("%.2f", order.getPrice()),
    String.format("%.2f", runningAverage), orderCount);
```

**Talking points:**
> "As each order is processed, we calculate a running average of all prices in real-time. This demonstrates stream processing capabilities."

**3. Retry Logic:**
```java
// Lines 97-117
private void handleRetry(ConsumerRecord<String, Order> record, Exception e) {
    String key = record.key();
    int attempts = retryAttempts.getOrDefault(key, 0) + 1;

    if (attempts <= MAX_RETRY_ATTEMPTS) {
        retryAttempts.put(key, attempts);
        Thread.sleep(1000 * attempts);  // Exponential backoff

        try {
            processOrder(record);
            retryAttempts.remove(key);
        } catch (Exception retryException) {
            if (attempts >= MAX_RETRY_ATTEMPTS) {
                sendToDlq(record, retryException);
            }
        }
    }
}
```

**Talking points:**
> "If processing fails, we retry up to 3 times with exponential backoff (1s, 2s, 3s). This handles temporary failures like network issues. I've added a 10% simulated failure rate to demonstrate this."

**4. Dead Letter Queue:**
```java
// Lines 122-139
private void sendToDlq(ConsumerRecord<String, Order> record, Exception e) {
    Order order = record.value();
    String dlqMessage = String.format(
        "Failed Order - ID: %s, Product: %s, Price: %.2f | Error: %s",
        order.getOrderId(), order.getProduct(),
        order.getPrice(), e.getMessage()
    );

    ProducerRecord<String, String> dlqRecord =
        new ProducerRecord<>(DLQ_TOPIC, record.key(), dlqMessage);

    dlqProducer.send(dlqRecord).get();
    logger.error("✗ Sent to DLQ - Order ID: {} after {} attempts",
        order.getOrderId(), MAX_RETRY_ATTEMPTS);
}
```

**Talking points:**
> "After 3 failed retry attempts, the message goes to a Dead Letter Queue. This prevents blocking the main pipeline while preserving failed messages for investigation."

---

### Part 3: Live Demonstration (5-7 minutes)

#### Step 3.1: Start the Consumer

**Terminal 1:**
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
run-consumer.bat
```

**What you'll see:**
```
[INFO] Starting Order Consumer...
[INFO] Started consuming from topic: orders
[INFO] DLQ Topic: orders-dlq
[INFO] Max Retry Attempts: 3
========================================
```

**Talking points:**
> "The consumer is now listening to the 'orders' topic. It's configured to handle retries and send failed messages to 'orders-dlq' topic."

#### Step 3.2: Start the Producer

**Terminal 2:**
```cmd
cd "C:\Users\parak\OneDrive\Desktop\Modules\8th sem\BigData\Inclass_Assignment_2"
run-producer.bat
```

**What you'll see:**
```
[INFO] Starting to produce 10 orders...
[INFO] Sent order: OrderID=1001, Product=Laptop, Price=$543.21 -> Partition=0, Offset=0
[INFO] Sent order: OrderID=1002, Product=Phone, Price=$289.45 -> Partition=0, Offset=1
...
```

**Talking points:**
> "The producer is generating 10 orders with random products and prices. Each message is serialized using Avro and sent to Kafka."

#### Step 3.3: Watch Real-Time Processing

**Switch to Terminal 1 (Consumer)** and observe:

**Example Output:**
```
[INFO] ✓ Processed Order: ID=1001, Product=Laptop, Price=$543.21 | Running Avg: $543.21 (Total Orders: 1)
[INFO] ✓ Processed Order: ID=1002, Product=Phone, Price=$289.45 | Running Avg: $416.33 (Total Orders: 2)
[WARN] ⚠ Retry attempt 1/3 for Order ID: 1003 - Error: Simulated temporary processing failure
[INFO] ✓ Retry successful for Order ID: 1003
[INFO] ✓ Processed Order: ID=1003, Product=Monitor, Price=$412.78 | Running Avg: $415.15 (Total Orders: 3)
[INFO] ✓ Processed Order: ID=1004, Product=Keyboard, Price=$125.99 | Running Avg: $342.86 (Total Orders: 4)
[WARN] ⚠ Retry attempt 1/3 for Order ID: 1005
[WARN] ⚠ Retry attempt 2/3 for Order ID: 1005
[WARN] ⚠ Retry attempt 3/3 for Order ID: 1005
[ERROR] ✗ Sent to DLQ - Order ID: 1005 after 3 attempts. Error: Simulated temporary processing failure
```

**Point out key observations:**

1. **✓ Successful Processing:**
   > "See how each order updates the running average in real-time. Order 1 was $543, then averaging with Order 2 ($289) gives us $416."

2. **⚠ Retry Attempts:**
   > "Notice Order 1003 failed initially but succeeded on retry. The exponential backoff gives temporary issues time to resolve."

3. **✗ DLQ Messages:**
   > "Order 1005 failed all 3 retry attempts and was sent to the Dead Letter Queue. This prevents it from blocking other messages."

4. **📊 Running Average:**
   > "The running average is calculated in real-time across all successfully processed orders."

---

### Part 4: Verification & Monitoring (3-4 minutes)

#### Step 4.1: Check Kafka Topics

```cmd
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Expected output:**
```
orders
orders-dlq
__consumer_offsets
_schemas
```

**Talking points:**
> "We can see our two application topics: 'orders' (main topic) and 'orders-dlq' (dead letter queue). The system topics are for Kafka's internal use."

#### Step 4.2: View DLQ Messages

```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-dlq --from-beginning
```

**Expected output:**
```
Failed Order - ID: 1005, Product: Tablet, Price: 678.90 | Error: Simulated temporary processing failure | Original Partition: 0, Offset: 4
Failed Order - ID: 1008, Product: Mouse, Price: 45.50 | Error: Simulated temporary processing failure | Original Partition: 0, Offset: 7
```

**Talking points:**
> "The DLQ contains detailed information about failed messages including the original error, partition, and offset. This helps with debugging and manual reprocessing."

#### Step 4.3: Check Schema Registry

```cmd
curl http://localhost:8081/subjects
```

**Expected output:**
```json
["orders-value"]
```

```cmd
curl http://localhost:8081/subjects/orders-value/versions/latest
```

**Expected output:**
```json
{
  "subject": "orders-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.bigdata.kafka.avro\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"product\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"float\"}]}"
}
```

**Talking points:**
> "Schema Registry automatically registered our Avro schema. It maintains version history and ensures schema compatibility across producers and consumers."

#### Step 4.4: Check Consumer Group

```cmd
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group order-consumer-group --describe
```

**Expected output:**
```
GROUP              TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-consumer-group orders   0          10              10              0
```

**Talking points:**
> "Consumer lag is 0, meaning all messages have been processed. Current offset shows we've consumed 10 messages."

---

## What to Explain During Demo

### Key Concepts to Highlight

#### 1. Avro Serialization
> "Unlike JSON which sends the schema with every message, Avro uses Schema Registry. The schema is registered once, and messages only contain the schema ID. This makes messages much smaller and faster to process."

#### 2. At-Least-Once Delivery
> "We disable auto-commit and manually commit offsets after successful processing. If the consumer crashes, it will reprocess some messages rather than lose data."

#### 3. Exponential Backoff
> "The retry delays increase: 1 second, 2 seconds, 3 seconds. This prevents overwhelming a struggling system while giving transient issues time to resolve."

#### 4. Dead Letter Queue Pattern
> "Instead of blocking the entire pipeline or losing failed messages, we isolate them in a separate queue. Operations teams can investigate and potentially reprocess them."

#### 5. Real-Time Aggregation
> "The running average updates with each message. In a production system, you might track hourly averages, daily totals, or other windowed aggregations."

---

## Expected Output Examples

### Successful Processing Flow
```
Producer:
[INFO] Sent order: OrderID=1001, Product=Laptop, Price=$543.21 -> Partition=0, Offset=0

Consumer:
[INFO] ✓ Processed Order: ID=1001, Product=Laptop, Price=$543.21 | Running Avg: $543.21 (Total Orders: 1)
```

### Retry Flow (Temporary Failure → Success)
```
Consumer:
[WARN] ⚠ Retry attempt 1/3 for Order ID: 1003 - Error: Simulated temporary processing failure
[INFO] ✓ Retry successful for Order ID: 1003
[INFO] ✓ Processed Order: ID=1003, Product=Monitor, Price=$412.78 | Running Avg: $415.15 (Total Orders: 3)
```

### DLQ Flow (Permanent Failure)
```
Consumer:
[WARN] ⚠ Retry attempt 1/3 for Order ID: 1005 - Error: Simulated temporary processing failure
[WARN] ⚠ Retry attempt 2/3 for Order ID: 1005 - Error: Simulated temporary processing failure
[WARN] ⚠ Retry attempt 3/3 for Order ID: 1005 - Error: Simulated temporary processing failure
[ERROR] ✗ Sent to DLQ - Order ID: 1005 after 3 attempts. Error: Simulated temporary processing failure
```

### Final Statistics
```
Consumer (on shutdown):
[INFO] Closing consumer...
[INFO] Final Statistics:
[INFO] Total Orders Processed: 8
[INFO] Overall Average Price: $387.42
```

---

## Common Questions & Answers

### Q1: Why use Avro instead of JSON?
**Answer:**
> "Avro provides several advantages:
> - **Smaller messages**: Binary format is 30-50% smaller
> - **Faster**: No parsing overhead
> - **Type safety**: Compile-time validation
> - **Schema evolution**: Version compatibility checking
> - For this demo with 10 messages, the difference is small, but at scale (millions of messages), this significantly reduces storage and network costs."

### Q2: What happens if the consumer crashes?
**Answer:**
> "Because we use manual offset commits, Kafka remembers the last successfully processed message. When the consumer restarts, it picks up from there. Some messages might be reprocessed (at-least-once delivery), but none are lost."

### Q3: How does the retry logic know when to give up?
**Answer:**
> "After 3 failed attempts with exponential backoff (totaling ~6 seconds), the message goes to DLQ. In production, you'd configure this based on your SLA. For transient network issues, 3 retries over 6 seconds is usually sufficient."

### Q4: Can you add more consumers to process faster?
**Answer:**
> "Yes! Kafka consumers work in groups. If you have multiple partitions, you can run multiple consumers in the same consumer group. Each partition is assigned to one consumer, enabling horizontal scaling."

### Q5: What if the schema changes?
**Answer:**
> "Schema Registry enforces compatibility rules. You can add optional fields (backward compatible) or remove unused fields (forward compatible). Breaking changes require a new schema version and coordinated deployment."

### Q6: How do you monitor this in production?
**Answer:**
> "You'd monitor:
> - Consumer lag (are we falling behind?)
> - Error rates (how many messages go to DLQ?)
> - Processing latency (how long to process each message?)
> - Schema Registry health
> - Kafka broker metrics
> Tools like Prometheus, Grafana, and Kafka Manager are commonly used."

### Q7: Why simulate failures? Can you show a real failure?
**Answer:**
> "The 10% simulated failure rate demonstrates the retry mechanism without requiring complex setup. In production, real failures might include:
> - Database connection timeouts
> - External API rate limits
> - Validation errors
> - Downstream service outages
> The retry logic handles all of these the same way."

---

## Troubleshooting During Demo

### Issue 1: Consumer doesn't receive messages

**Symptoms:**
- Producer sends messages successfully
- Consumer shows "Started consuming..." but no processing

**Check:**
```cmd
# Verify consumer is in the same network
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Should show: order-consumer-group
```

**Solution:**
- Restart consumer
- Check consumer group ID matches
- Verify topics exist

### Issue 2: Schema Registry errors

**Symptoms:**
```
Error: SchemaRegistryException: Error while registering schema
```

**Check:**
```cmd
docker logs schema-registry
curl http://localhost:8081/subjects
```

**Solution:**
- Restart Schema Registry
- Check Schema Registry URL in code
- Clear schemas if needed

### Issue 3: Maven build fails

**Symptoms:**
```
[ERROR] Failed to execute goal
```

**Solution:**
```cmd
# Clean and rebuild
mvn clean compile

# If persists, delete target folder manually
rmdir /s /q target
mvn compile
```

### Issue 4: Docker containers not running

**Symptoms:**
```
docker ps  # Shows empty or missing containers
```

**Solution:**
```cmd
docker-compose down
docker-compose up -d
docker ps  # Verify all 3 containers are up
```

### Issue 5: Port conflicts

**Symptoms:**
```
Error: Port 9092 is already in use
```

**Solution:**
```cmd
# Find process using port
netstat -ano | findstr :9092

# Kill the process or use different ports in docker-compose.yml
```

---

## Demonstration Checklist

### Before Demo
- [ ] All Docker containers running
- [ ] Maven in PATH
- [ ] Project compiled
- [ ] Two terminals ready
- [ ] Code editor open

### During Demo
- [ ] Show project structure
- [ ] Explain Avro schema
- [ ] Walk through producer code
- [ ] Walk through consumer code
- [ ] Start consumer first
- [ ] Start producer
- [ ] Point out running average
- [ ] Show retry attempts
- [ ] Show DLQ messages
- [ ] Check Kafka topics
- [ ] View Schema Registry
- [ ] Check consumer lag

### After Demo
- [ ] Answer questions
- [ ] Show final statistics
- [ ] Discuss production considerations
- [ ] Stop containers if needed

---

## Presentation Tips

### Pacing
1. **Setup & Code** (5 min): Don't rush, explain concepts
2. **Live Demo** (7 min): Let it run, narrate what's happening
3. **Verification** (3 min): Show the data flow
4. **Q&A** (5 min): Engage with audience

### What to Emphasize
- **Schema Evolution**: Enterprise benefit of Avro
- **Fault Tolerance**: Retry logic saves real money
- **Observability**: DLQ enables debugging
- **Scalability**: Consumer groups enable horizontal scaling

### Common Mistakes to Avoid
- ❌ Starting producer before consumer (messages might be missed)
- ❌ Not explaining the running average calculation
- ❌ Skipping the DLQ verification
- ❌ Forgetting to mention manual offset management

---

## Success Criteria

Your demonstration is successful if you show:

1. ✅ **Avro Serialization**: Schema Registry integration working
2. ✅ **Real-Time Aggregation**: Running average updates visibly
3. ✅ **Retry Logic**: At least one retry attempt shown
4. ✅ **Dead Letter Queue**: At least one message in DLQ
5. ✅ **End-to-End Flow**: Producer → Kafka → Consumer → Processing
6. ✅ **Monitoring**: Topics, consumer groups, schemas verified

---

## Final Notes

**Time Management:**
- Aim for 15-20 minutes total
- Leave 5 minutes for questions
- Have backup if live demo fails (screenshots/video)

**Confidence Builders:**
- Test the full flow before the actual demo
- Have the code and commands ready in a text file
- Know your retry intervals and DLQ logic cold
- Be ready to explain "why" not just "what"

**Remember:**
> You've built a production-quality system with enterprise patterns. Be confident in explaining the design decisions and trade-offs!

Good luck with your demonstration! 🚀
