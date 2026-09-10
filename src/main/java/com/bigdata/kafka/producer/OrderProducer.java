package com.bigdata.kafka.producer;

import com.bigdata.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Producer that generates and sends Order messages with Avro serialization
 */
public class OrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);
    private static final String TOPIC = "orders";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private final KafkaProducer<String, Order> producer;
    private final Random random;

    public OrderProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        // Producer reliability configurations
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        this.producer = new KafkaProducer<>(props);
        this.random = new Random();
    }

    /**
     * Generate and send order messages
     */
    public void produceOrders(int numberOfOrders) {
        String[] products = {"Laptop", "Phone", "Tablet", "Headphones", "Monitor",
                           "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"};

        for (int i = 1; i <= numberOfOrders; i++) {
            try {
                // Create Order using Avro generated class
                Order order = Order.newBuilder()
                        .setOrderId(String.valueOf(1000 + i))
                        .setProduct(products[random.nextInt(products.length)])
                        .setPrice(random.nextFloat() * 1000 + 10) // Price between 10 and 1010
                        .build();

                // Create producer record
                ProducerRecord<String, Order> record = new ProducerRecord<>(TOPIC, order.getOrderId().toString(), order);

                // Send message synchronously for demonstration
                RecordMetadata metadata = producer.send(record).get();

                logger.info("Sent order: OrderID={}, Product={}, Price=${} -> Partition={}, Offset={}",
                        order.getOrderId(),
                        order.getProduct(),
                        String.format("%.2f", order.getPrice()),
                        metadata.partition(),
                        metadata.offset());

                // Small delay for readability in demo
                Thread.sleep(1000);

            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error producing order: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Close producer resources
     */
    public void close() {
        logger.info("Closing producer...");
        producer.close();
    }

    public static void main(String[] args) {
        OrderProducer orderProducer = new OrderProducer();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(orderProducer::close));

        // Produce 20 orders by default
        int numberOfOrders = args.length > 0 ? Integer.parseInt(args[0]) : 20;

        logger.info("Starting to produce {} orders...", numberOfOrders);
        orderProducer.produceOrders(numberOfOrders);

        orderProducer.close();
    }
}
