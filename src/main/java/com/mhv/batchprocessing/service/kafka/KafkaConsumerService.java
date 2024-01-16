package com.mhv.batchprocessing.service.kafka;

import com.mhv.batchprocessing.service.customer.CustomerJobService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
@Service
public class KafkaConsumerService {

    @Autowired
    private CustomerJobService customerJobService;

    @KafkaListener(id = "customerCtlListener", topics = "${kafka.csv.topic.customer_ctl}", containerFactory = "kafkaCustomerCtlListenerContainerFactoryBean", groupId = ("${kafka.customer.ctl.group.id}"))
    public void consume(ConsumerRecord<String, String> record) {
        System.out.println("********* [ KEY : " + record.key() + " | PARTITION : " + record.partition() + " | VALUE : " + record.value());
        try {
            customerJobService.triggerCustomerTransformationAndLoadJob(record.key());
        }catch (Exception e){
            System.out.println("Exception in starting transformation-load job : " + e.getMessage());
        }
    }
}
