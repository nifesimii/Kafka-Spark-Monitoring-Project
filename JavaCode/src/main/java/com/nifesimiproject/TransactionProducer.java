package com.nifesimiproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nifesimiproject.dto.Transaction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.datatransfer.StringSelection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionProducer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String TOPIC = "financial_transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092, localhost:49092";
    private static final int NUM_THREADS = 3;
    private static final int NUM_PARTITIONS = 5; // Define the number of partitions
    private static final short REPLICATION_FACTOR = 3;   // Define the replication factor

    public static void main(String[] args) {
        // Check and create topic if it doesn't exist
        createTopicIfNotExists();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); //64MB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 3); // 5MS
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        ObjectMapper objectMapper= new ObjectMapper();

        for(int i = 0 ; i < NUM_THREADS; i++){
            executor.submit( () -> {
                long startTime = System.currentTimeMillis();
                long recordsSent = 0;


                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)){
                    while (true){
                        Transaction transaction = Transaction.randomTransaction();
                        String transactionJson =  objectMapper.writeValueAsString(transaction);
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, transaction.getTransactionId(), transactionJson);
                        producer.send(record, (RecordMetadata metadata , Exception exception) -> {
                            if (exception != null){
                                logger.error("failed to send record with key {}  due to {}", record.key(), exception.getMessage());
                            }else{
                                logger.info("Record with key {} sent to partition {} with offset {}", record.key(), metadata.partition(), metadata.offset());

                            }
                        });
                        recordsSent++;

                        // Track throughput every second
                        long elapsedTime = System.currentTimeMillis() - startTime;
                        if (elapsedTime >= 1000) {
                            double throughput = recordsSent / (elapsedTime / 1000.0);
                            logger.info("Throughput: {} records/sec", throughput);
                            recordsSent = 0;
                            startTime = System.currentTimeMillis();
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error in producer thread", e);
                }
            });
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdownNow();
            logger.info("Producer stopped.");
        }));

    }


    private static void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean topicExists = adminClient.listTopics().names().get().contains(TOPIC);

            if (!topicExists) {
                logger.info("Topic '{}' does not exist . Creating topic...", TOPIC);
                NewTopic newTopic = new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                logger.info("Topic '{}' created sucessfully.", TOPIC);
            } else {
                logger.info("Topic '{}' already exists.", TOPIC);
            }
        }catch (Exception e){
            logger.error("Error checking or creating topic", e);
        }

    }
}
