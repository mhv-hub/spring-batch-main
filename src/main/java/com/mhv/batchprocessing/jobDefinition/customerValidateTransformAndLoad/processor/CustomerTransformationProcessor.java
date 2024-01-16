package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.processor;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.entity.TransformedCustomer;
import com.mhv.batchprocessing.service.customer.CustomerTransformationService;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "customerTransformationProcessorBean")
public class CustomerTransformationProcessor implements ItemProcessor<Customer, TransformedCustomer> {

    @Autowired
    private CustomerTransformationService customerTransformationService;

    @Override
    public TransformedCustomer process(Customer customer) throws Exception {
        TransformedCustomer transformedCustomer = new TransformedCustomer(customer);
        customerTransformationService.transformCustomerName(transformedCustomer, customer.getCustomerName(), customer.getCustomerType());
        customerTransformationService.transformLocation(transformedCustomer, customer.getLocation());
        customerTransformationService.transformMembershipStatus(transformedCustomer, customer.getJoiningDate());
        return transformedCustomer;
    }
}