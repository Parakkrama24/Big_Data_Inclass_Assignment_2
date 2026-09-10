package com.bigdata.kafka.consumer;

import com.bigdata.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Consumer that processes Order messages with:
 * - Real-time price aggregation (running average)
 * - Retry logic for temporary failures
 * - Dead Letter Queue for permanently failed messages
 */
public class OrderConsumer {
    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);

    private static final String ORDERS_TOPIC = "orders";
    private static final String RETRY_TOPIC = "orders-retry";
    private static final String DLQ_TOPIC = "orders-dlq";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String GROUP_ID = "order-consumer-group";

    private static final int MAX_RETRY_ATTEMPTS = 3;

    private final KafkaConsumer<String, Order> consumer;
    private final KafkaProducer<String, String> dlqProducer;
    private final Map<String, Integer> retryAttempts;

    // Aggregation state
    private double totalPrice = 0.0;
    private int orderCount = 0;

    public OrderConsumer() {
        this.consumer = createConsumer();
        this.dlqProducer = createDlqProducer();
        this.retryAttempts = new HashMap<>();
    }

    /**
     * Create and configure Kafka consumer
     */
    private KafkaConsumer<String, Order> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // Consumer configurations
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return new KafkaConsumer<>(props);
    }

    /**
     * Create DLQ producer for failed messages
     */
    private KafkaProducer<String, String> createDlqProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(props);
    }

    /**
     * Process order and update running average
     */
    private void processOrder(ConsumerRecord<String, Order> record) throws Exception {
        Order order = record.value();

        // Simulate potential processing failures (10% failure rate for demonstration)
        if (Math.random() < 0.1) {
            throw new Exception("Simulated temporary processing failure");
        }

        // Update aggregation
        orderCount++;
        totalPrice += order.getPrice();
        double runningAverage = totalPrice / orderCount;

        logger.info("✓ Processed Order: ID={}, Product={}, Price=${} | Running Avg: ${} (Total Orders: {})",
                order.getOrderId(),
                order.getProduct(),
                String.format("%.2f", order.getPrice()),
                String.format("%.2f", runningAverage),
                orderCount);
    }

    /**
     * Handle retry logic
     */
    private void handleRetry(ConsumerRecord<String, Order> record, Exception e) {
        String key = record.key();
        int attempts = retryAttempts.getOrDefault(key, 0) + 1;

        if (attempts <= MAX_RETRY_ATTEMPTS) {
            retryAttempts.put(key, attempts);
            logger.warn("⚠ Retry attempt {}/{} for Order ID: {} - Error: {}",
                    attempts, MAX_RETRY_ATTEMPTS, key, e.getMessage());

            // In a real system, you might send to a retry topic with delay
            try {
                Thread.sleep(1000 * attempts); // Exponential backoff simulation
                processOrder(record);
                retryAttempts.remove(key); // Success, remove from retry tracking
                logger.info("✓ Retry successful for Order ID: {}", key);
            } catch (Exception retryException) {
                if (attempts >= MAX_RETRY_ATTEMPTS) {
                    sendToDlq(record, retryException);
                }
            }
        } else {
            sendToDlq(record, e);
        }
    }

    /**
     * Send failed message to Dead Letter Queue
     */
    private void sendToDlq(ConsumerRecord<String, Order> record, Exception e) {
        Order order = record.value();
        String dlqMessage = String.format(
                "Failed Order - ID: %s, Product: %s, Price: %.2f | Error: %s | Original Partition: %d, Offset: %d",
                order.getOrderId(),
                order.getProduct(),
                order.getPrice(),
                e.getMessage(),
                record.partition(),
                record.offset()
        );

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(DLQ_TOPIC, record.key(), dlqMessage);

        try {
            dlqProducer.send(dlqRecord).get();
            retryAttempts.remove(record.key());
            logger.error("✗ Sent to DLQ - Order ID: {} after {} attempts. Error: {}",
                    order.getOrderId(), MAX_RETRY_ATTEMPTS, e.getMessage());
        } catch (Exception dlqException) {
            logger.error("Failed to send message to DLQ: {}", dlqException.getMessage());
        }
    }

    /**
     * Start consuming messages
     */
    public void consume() {
        consumer.subscribe(Collections.singletonList(ORDERS_TOPIC));
        logger.info("Started consuming from topic: {}", ORDERS_TOPIC);
        logger.info("DLQ Topic: {}", DLQ_TOPIC);
        logger.info("Max Retry Attempts: {}", MAX_RETRY_ATTEMPTS);
        logger.info("========================================\n");

        try {
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Order> record : records) {
                    try {
                        processOrder(record);
                    } catch (Exception e) {
                        handleRetry(record, e);
                    }
                }

                // Commit offsets after processing batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            logger.error("Consumer error: {}", e.getMessage(), e);
        } finally {
            close();
        }
    }

    /**
     * Close consumer and producer resources
     */
    public void close() {
        logger.info("\nClosing consumer...");
        logger.info("Final Statistics:");
        logger.info("Total Orders Processed: {}", orderCount);
        if (orderCount > 0) {
            logger.info("Overall Average Price: ${}", String.format("%.2f", totalPrice / orderCount));
        }
        consumer.close();
        dlqProducer.close();
    }

    public static void main(String[] args) {
        OrderConsumer orderConsumer = new OrderConsumer();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(orderConsumer::close));

        orderConsumer.consume();
    }
}
