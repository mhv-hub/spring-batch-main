package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.mhv.batchprocessing.entity.TransformedCustomer;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "customerWriteListenerBean")
public class CustomerDatabaseWriteListener implements ItemWriteListener<TransformedCustomer> {

    @Override
    public void beforeWrite(Chunk<? extends TransformedCustomer> items) {
    }

    @Override
    public void afterWrite(Chunk<? extends TransformedCustomer> items) {
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends TransformedCustomer> items) {
    }
}
