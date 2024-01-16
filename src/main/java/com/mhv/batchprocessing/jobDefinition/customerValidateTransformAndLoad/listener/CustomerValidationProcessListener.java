package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.mhv.batchprocessing.entity.Customer;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "customerValidationProcessListenerBean")
public class CustomerValidationProcessListener implements ItemProcessListener<Customer, Customer> {

    @Override
    public void beforeProcess(Customer item) {
    }

    @Override
    public void afterProcess(Customer item, Customer result) {
    }

    @Override
    public void onProcessError(Customer item, Exception e) {
    }
}
