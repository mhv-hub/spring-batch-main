package com.mhv.batchprocessing;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.scope.StepScope;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:kafka.properties")
@EnableBatchProcessing
public class BatchProcessingApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchProcessingApplication.class, args);
	}
}
