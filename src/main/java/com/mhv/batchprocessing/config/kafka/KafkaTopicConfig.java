package com.mhv.batchprocessing.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.csv.topic.customer}")
    private String customerDataTopic;

    @Value("${kafka.csv.topic.customer_ctl}")
    private String customerCtlTopic;

    @Value("${kafka.customer.topic.partition.count}")
    private int partitionCount;

    @Bean
    public NewTopic createCustomerDataTopic(){
        return TopicBuilder.name(customerDataTopic).partitions(partitionCount).build();
    }

    @Bean
    public NewTopic createCustomerCtlTopic(){
        return TopicBuilder.name(customerCtlTopic).partitions(partitionCount).build();
    }
}
