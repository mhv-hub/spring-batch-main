package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.entity.TransformedCustomer;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "transformationProcessListenerBean")
public class TransformationProcessListener implements ItemProcessListener<Customer, TransformedCustomer> {
    @Override
    public void beforeProcess(Customer item) {
    }

    @Override
    public void afterProcess(Customer item, TransformedCustomer result) {
    }

    @Override
    public void onProcessError(Customer item, Exception e) {
    }
}
