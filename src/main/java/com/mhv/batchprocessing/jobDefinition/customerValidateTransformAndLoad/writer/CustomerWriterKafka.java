package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.writer;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.service.kafka.KafkaProducerService;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.ArrayList;

@Component
@Qualifier(value = "customerMessageEventWriter")
@StepScope
public class CustomerWriterKafka implements ItemWriter<Customer> {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Value("${kafka.csv.topic.customer}")
    private String customerDataTopic;

    @Value("${kafka.csv.topic.customer_ctl}")
    private String customerCtlTopic;

    @Value("#{stepExecution.jobExecution}")
    private JobExecution jobExecution;

    @Override
    public void write(Chunk<? extends Customer> customers) throws Exception {
        Exception exception = kafkaProducerService.pushCustomerDataToQueue(new ArrayList<>(customers.getItems()), String.valueOf(jobExecution.getJobParameters().getLong("jobKey")), customerDataTopic);
        System.out.println(
                exception == null ?
                        "[ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Customer data published to message queue [ " + LocalTime.now() + " ]" :
                        "[ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Issue in pushing customer data to message queue : [ " + LocalTime.now() + " ] [ " + exception.getMessage() + " ]"
        );
    }
}
