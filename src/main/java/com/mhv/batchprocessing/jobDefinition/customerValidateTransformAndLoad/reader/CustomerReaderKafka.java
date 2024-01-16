package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.reader;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.util.CustomerJsonDeserializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class CustomerReaderKafka {

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaServer;

    @Value("${kafka.customer.data.group.id}")
    private String groupId;

    @Value("${kafka.csv.topic.customer}")
    private String topic;

    @Value("${kafka.customer.topic.partition.count}")
    private int partitionCount;

    @Bean(name = "customerKafkaQueueReaderBean")
    @StepScope
    public KafkaItemReader<String, Customer> itemReader(@Value("#{jobParameters[jobKey]}") String key){
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerJsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<Integer> desiredPartitions = null;
        try(AdminClient adminClient = AdminClient.create(config)){
            List<TopicPartitionInfo> partitions = adminClient.describeTopics(Collections.singletonList(topic))
                    .topicNameValues()
                    .get(topic)
                    .get()
                    .partitions();
            desiredPartitions = partitions.stream()
                    .filter(topicPartitionInfo -> topicPartitionInfo.leader() != null)
                    .map(TopicPartitionInfo::partition)
                    .filter(partition -> Math.abs(Integer.parseInt(key) % partitionCount) == partition)
                    .toList();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new KafkaItemReaderBuilder<String, Customer>()
                .partitions(desiredPartitions)
                .partitionOffsets(new HashMap<>())
                .consumerProperties(config)
                .name("kafkaItemReader")
                .topic(topic)
                .build();
    }
}
