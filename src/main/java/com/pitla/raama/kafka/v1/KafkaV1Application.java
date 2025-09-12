package com.pitla.raama.kafka.v1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaV1Application {

	public static void main(String[] args) {
		System.out.println("about start APP");
		SpringApplication.run(KafkaV1Application.class, args);
	}

}
