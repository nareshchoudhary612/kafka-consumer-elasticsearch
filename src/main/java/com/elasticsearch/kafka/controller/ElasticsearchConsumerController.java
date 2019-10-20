package com.elasticsearch.kafka.controller;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

//import com.elasticsearch.kafka.consumer.ElasticsearchConsumer;
import com.elasticsearch.kafka.service.KafkaToECService;

@RestController
public class ElasticsearchConsumerController {

	@Autowired
	KafkaToECService kafkaToECService;

	ExecutorService executorService = Executors.newFixedThreadPool(1);

	/* Starting Kafka consumer to consume messages and send them to ElasticSearch */
	@GetMapping("/startConsumer")
	public ResponseEntity startConsumer() throws IOException {

		kafkaToECService.startSending();
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}

	@GetMapping("/stopConsumer")
	public ResponseEntity stopConsumer() throws IOException {

		// To be implemented
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}

	/*
	 * Starting Kafka consumer to consume messages and send them to ElasticSearch as
	 * background service
	 */
	@GetMapping("/startConsumerInBackground")
	public ResponseEntity startConsumerInBackground() {

		executorService.execute(kafkaToECService);
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}

	/* Stopping the background running service */
	@GetMapping("/stopConsumerInBackground")
	public ResponseEntity stopConsumerInBackground() {

		executorService.shutdownNow();
		return new ResponseEntity<>(HttpStatus.ACCEPTED);
	}

}
