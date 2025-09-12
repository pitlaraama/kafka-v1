package com.pitla.raama.kafka.v1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.pitla.raama.kafka.v1.config.KafkaConfiguration;

public class Producer {

    private final KafkaProducer<String, String> producer;

    public Producer() {
        this.producer = KafkaConfiguration.getProducer();
    }

    public void sendMessage(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            RecordMetadata metadata = producer.send(record).get();
            logSuccess(key, value, metadata);
        } catch (Exception e) {
            logError(e);
        }
    }

    private void logSuccess(String key, String value, RecordMetadata metadata) {
        System.out.printf("Sent record(key=%s value=%s) to partition %d, offset %d%n",
                key, value, metadata.partition(), metadata.offset());
    }

    private void logError(Exception e) {
        System.err.println("Error sending record: " + e.getMessage());
        e.printStackTrace();
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        Producer producerApp = new Producer();
        String topic = "test-topic";
        String key = "sample-key";
        String value = "Hello, Kafka!";

        producerApp.sendMessage(topic, key, value);
    }
}
