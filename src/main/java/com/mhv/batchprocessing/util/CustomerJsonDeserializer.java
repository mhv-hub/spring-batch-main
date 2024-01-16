package com.mhv.batchprocessing.util;

import com.mhv.batchprocessing.entity.Customer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import java.util.Arrays;

public class CustomerJsonDeserializer extends JsonDeserializer<Customer> {

    @Override
    public Customer deserialize(String topic, byte[] data) {
        try {
            return super.deserialize(topic, data);
        } catch (Exception e) {
            System.out.println("Exception in deserializing data [ TOPIC : " + topic + " DATA : " + Arrays.toString(data) + " ]");
            return null;
        }
    }
}
