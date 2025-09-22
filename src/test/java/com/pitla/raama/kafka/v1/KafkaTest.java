package com.pitla.raama.kafka.v1;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import com.pitla.raama.kafka.v1.consumer.Consumer;
import com.pitla.raama.kafka.v1.producer.Producer;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class KafkaTest {

	private static Producer producerApp;
	private static Consumer consumerApp;
	private static final String TOPIC = "raama-kafka";
	private static final String GROUP_ID = "test-group-raama";

	@BeforeAll
	static void setup() {
		producerApp = new Producer();
		consumerApp = new Consumer(GROUP_ID, TOPIC);
	}

	@AfterAll
	static void cleanup() {
		producerApp.close();
		// Consumer closes automatically after pollMessages loop ends
	}

	@Test
	void testProducerSendsMessage() {
		String key = "test-key";
		String value = "test-value-producer";
		assertDoesNotThrow(() -> producerApp.sendMessage(TOPIC, key, value));
	}

	@Test
	void testConsumerReceivesMessage() throws InterruptedException {
		String key = "test-key";
		String value = "test-value=rama-pc";
		producerApp.sendMessage(TOPIC, key, value);

		boolean messageReceived = false;
		long timeout = System.currentTimeMillis() + 5000; // 5 seconds timeout

		while (System.currentTimeMillis() < timeout) {
			// You may need to expose a method in Consumer to poll and return records for
			// testing
			// For demonstration, let's assume you add a pollAndCheckMessage method:
			messageReceived = pollAndCheckMessage(consumerApp, key, value);
			if (messageReceived)
				break;
			Thread.sleep(500);
		}

		assertTrue(messageReceived, "Consumer should receive the produced message");
	}

	// Helper method for testing
	private boolean pollAndCheckMessage(Consumer consumer, String key, String value) {
		consumer.pollMessages();
		return false;
	}
}
