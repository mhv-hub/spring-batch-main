package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.mhv.batchprocessing.entity.Customer;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "customerKafkaWriteListenerBean")
public class CustomerKafkaWriteListener implements ItemWriteListener<Customer> {

    @Override
    public void beforeWrite(Chunk<? extends Customer> items) {
    }

    @Override
    public void afterWrite(Chunk<? extends Customer> items) {
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends Customer> items) {
    }
}
