package com.pitla.raama.kafka.v1.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.pitla.raama.kafka.v1.config.KafkaConfiguration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;

public class Consumer {

    private final KafkaConsumer<String, String> consumer;

    public Consumer(String groupId, String topic) {
        this.consumer = KafkaConfiguration.getConsumer(groupId);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void pollMessages() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (Exception e) {
            logError(e);
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        System.out.printf("Received record(key=%s value=%s) from partition %d, offset %d%n",
                record.key(), record.value(), record.partition(), record.offset());
        // Add business logic here for processing the message
    }

    private void logError(Exception e) {
        System.err.println("Error consuming record: " + e.getMessage());
        e.printStackTrace();
    }

    public static void main(String[] args) {
        String groupId = "test-group";
        String topic = "test-topic";
        Consumer consumerApp = new Consumer(groupId, topic);
        consumerApp.pollMessages();
    }
}
