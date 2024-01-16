package com.mhv.batchprocessing.service.customer;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.exceptionHandeler.*;
import com.mhv.batchprocessing.util.CustomerStatus;
import com.mhv.batchprocessing.util.CustomerType;
import com.mhv.batchprocessing.util.Gender;
import com.mhv.batchprocessing.util.Location;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class CustomerValidationService {

    private final static int CUSTOMER_NAME_MAX_LENGTH = 100;
    private final static int CUSTOMER_GENDER_MAX_LENGTH = 51;
    private final static int CUSTOMER_TYPE_MAX_LENGTH = 30;
    private final static int CUSTOMER_STATUS_MAX_LENGTH = 10;
    private final static int CUSTOMER_UNIQUE_ID_MAX_LENGTH = 10;

    public void validateCustomerUniqueId(Customer customer) throws FieldValueBlankException, FieldLengthOutOfRangeException, UnExpectedFieldValueException{
        if(customer.getUniqueId() == null || customer.getUniqueId().length() == 0){
            throw new FieldValueBlankException("Customer unique id is missing");
        }
        if(customer.getUniqueId().length() > CustomerValidationService.CUSTOMER_UNIQUE_ID_MAX_LENGTH){
            throw new FieldLengthOutOfRangeException("Customer unique id value has more than " + CustomerValidationService.CUSTOMER_UNIQUE_ID_MAX_LENGTH + " characters");
        }
        if(!customer.getUniqueId().matches("\\d+")){
            throw new UnExpectedFieldValueException("Customer unique id field has non numeric values. Only digits allowed");
        }
    }

    public void validateCustomerName(Customer customer) throws FieldValueBlankException, FieldLengthOutOfRangeException, UnExpectedFieldValueException {
        if(customer.getCustomerName() == null || customer.getCustomerName().length() == 0){
            throw new FieldValueBlankException("Customer name is missing");
        }
        if(customer.getCustomerName().length() > CustomerValidationService.CUSTOMER_NAME_MAX_LENGTH){
            throw new FieldLengthOutOfRangeException("Customer name value has more than " + CustomerValidationService.CUSTOMER_NAME_MAX_LENGTH + " characters");
        }
        if(!customer.getCustomerName().matches("^[\\p{L} .'-]+$")){
            throw new UnExpectedFieldValueException("Customer name has characters other than letters and spaces");
        }
    }

    public void validateCustomerGender(Customer customer) throws FieldValueBlankException, FieldLengthOutOfRangeException, UnExpectedFieldValueException, FieldValueMissingException, IllegalFieldValueException {
        if(customer.getCustomerGender() == null || customer.getCustomerGender().length() == 0) {
            throw new FieldValueBlankException("Customer gender is missing");
        }
        if(customer.getCustomerGender().length() > CustomerValidationService.CUSTOMER_GENDER_MAX_LENGTH){
            throw new FieldLengthOutOfRangeException("Customer gender value has more than " + CustomerValidationService.CUSTOMER_GENDER_MAX_LENGTH + " characters");
        }
        if(Arrays.stream(Gender.values()).noneMatch(value -> value.toString().equals(customer.getCustomerGender()))){
            throw new UnExpectedFieldValueException("Customer gender value is not in expected format");
        }
        if(customer.getCustomerType().equals(CustomerType.INDIVIDUAL.name()) && customer.getCustomerGender().equals(Gender.NA.name())){
            throw new FieldValueMissingException("Customer gender value is missing");
        }
        if(customer.getCustomerType().equals(CustomerType.ORGANISATION.name()) && !customer.getCustomerGender().equals(Gender.NA.name())){
            throw new IllegalFieldValueException("Customer gender value is provided for type organisation");
        }
    }

    public void validateCustomerType(Customer customer) throws FieldValueBlankException, FieldLengthOutOfRangeException, UnExpectedFieldValueException {
        if(customer.getCustomerType() == null || customer.getCustomerType().length() == 0){
            throw new FieldValueBlankException("Customer type is missing");
        }
        if(customer.getCustomerType().length() > CustomerValidationService.CUSTOMER_TYPE_MAX_LENGTH){
            throw new FieldLengthOutOfRangeException("Customer type value has more than " + CustomerValidationService.CUSTOMER_TYPE_MAX_LENGTH + " characters");
        }
        if(Arrays.stream(CustomerType.values()).noneMatch(value -> value.toString().equals(customer.getCustomerType()))){
            throw new UnExpectedFieldValueException("Customer type value is not in expected format");
        }
    }

    public void validateCustomerLocation(Customer customer) throws FieldValueBlankException, UnExpectedFieldValueException{
        if(customer.getLocation() == null || customer.getLocation().length() == 0){
            throw new FieldValueBlankException("Customer location is missing [ Record : " + customer + " ]");
        }
        if(Arrays.stream(Location.values()).noneMatch(value -> value.toString().equals(customer.getLocation()))){
            throw new UnExpectedFieldValueException("Customer type value is not in expected format");
        }
    }

    public void validateCustomerActiveStatus(Customer customer) throws FieldValueBlankException, FieldLengthOutOfRangeException, UnExpectedFieldValueException{
        if(customer.getActiveStatus() == null || customer.getActiveStatus().length() == 0){
            throw new FieldValueBlankException("Customer status is missing");
        }
        if(customer.getActiveStatus().length() > CustomerValidationService.CUSTOMER_STATUS_MAX_LENGTH){
            throw new FieldLengthOutOfRangeException("Customer status value has more than " + CustomerValidationService.CUSTOMER_STATUS_MAX_LENGTH + " characters");
        }
        if(Arrays.stream(CustomerStatus.values()).noneMatch(value -> value.toString().equals(customer.getActiveStatus()))){
            throw new UnExpectedFieldValueException("Customer status value is not in expected format");
        }
    }

    public void validateCustomerDateOfBirth(Customer customer) throws FieldValueBlankException,FieldValueMissingException, IllegalFieldValueException {
        if(customer.getCustomerDateOfBirth() == null){
            throw new FieldValueBlankException("Customer date of birth is empty");
        }
        if(customer.getCustomerType().equals(CustomerType.INDIVIDUAL.name()) && customer.getCustomerDateOfBirth().getYear() == 9999){
            throw new FieldValueMissingException("Customer date of birth value is missing");
        }
        if(customer.getCustomerType().equals(CustomerType.ORGANISATION.name()) && customer.getCustomerDateOfBirth().getYear() != 9999){
            throw new IllegalFieldValueException("Customer date of birth value is provided for type organisation");
        }
    }
    public void validateCustomerJoiningDate(Customer customer) throws FieldValueBlankException{
        if(customer.getJoiningDate() == null){
            throw new FieldValueBlankException("Customer joining date is empty");
        }
    }
}
