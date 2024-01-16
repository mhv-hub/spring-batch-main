package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.processor;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.exceptionHandeler.*;
import com.mhv.batchprocessing.service.customer.CustomerValidationService;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier(value = "customerCsvDataProcessor")
public class CustomerValidationProcessor implements ItemProcessor<Customer, Customer> {

    @Autowired
    private CustomerValidationService customerValidationService;

    @Override
    public Customer process(Customer customer) throws FieldValueBlankException, FieldLengthOutOfRangeException, UnExpectedFieldValueException, FieldValueMissingException, IllegalFieldValueException {
        customerValidationService.validateCustomerName(customer);
        customerValidationService.validateCustomerGender(customer);
        customerValidationService.validateCustomerType(customer);
        customerValidationService.validateCustomerLocation(customer);
        customerValidationService.validateCustomerActiveStatus(customer);
        customerValidationService.validateCustomerDateOfBirth(customer);
        customerValidationService.validateCustomerJoiningDate(customer);
        return customer;
    }


}
