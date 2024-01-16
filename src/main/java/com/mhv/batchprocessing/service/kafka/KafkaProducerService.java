package com.mhv.batchprocessing.service.kafka;

import com.mhv.batchprocessing.entity.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaProducerService {

    @Autowired
    @Qualifier(value = "customerDataKafkaTemplateBean")
    private KafkaTemplate<String, Customer> customerDataKafkaTemplate;

    @Autowired
    @Qualifier(value = "customerCtlKafkaTemplateBean")
    private KafkaTemplate<String, String> customerCtlKafkaTemplate;

    public Exception pushCustomerDataToQueue(List<Customer> customerList, String key, String topic){
        if(customerList == null || customerList.size() == 0)
            return new Exception("No data provided");
        else if(topic == null || topic.length() == 0) {
            return new Exception("No topic provided");
        } else{
            try {
                customerList.forEach(customer -> customerDataKafkaTemplate.send(topic, key, customer));
                return null;
            }catch (Exception e){
                e.printStackTrace();
                return e;
            }
        }
    }

    public Exception pushCustomerCtlToQueue(String topic, String key){
        if(topic == null || topic.length() == 0){
            return new Exception("No data provided");
        }
        try{
            customerCtlKafkaTemplate.send(topic, key, "Customer data ready in queue");
            return null;
        }catch (Exception e){
            return e;
        }
    }
}
