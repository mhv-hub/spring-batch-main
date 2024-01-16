package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.entity.TransformedCustomer;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "transformationAndLoadSkipListenerBean")
@StepScope
public class TransformationAndLoadSkipListener implements SkipListener<Customer, TransformedCustomer> {
    @Override
    public void onSkipInRead(Throwable t) {
        System.out.println(">>>>>>>>>>>>>>> Error in Kafka customer read. Record skipped [ Error : " + t.getMessage() + " ]");
    }

    @Override
    public void onSkipInWrite(TransformedCustomer customer, Throwable t) {
        System.out.println(">>>>>>>>>>>>>>> Error in write. Record skipped [ Record : " + customer.getFirstName() + " ] [ Error : " + t.getMessage() + " ]");
    }

    @Override
    public void onSkipInProcess(Customer customer, Throwable t) {
        System.out.println(">>>>>>>>>>>>>>> Error in customer transformation. Record skipped [ Record : " + customer + " ] [ Error : " + t.getMessage() + " ]");
    }
}
