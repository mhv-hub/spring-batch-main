package com.mhv.batchprocessing.service.customer;

import com.mhv.batchprocessing.entity.TransformedCustomer;
import com.mhv.batchprocessing.util.CustomerType;
import com.mhv.batchprocessing.util.LocationToCountryMap;
import com.mhv.batchprocessing.util.MembershipStatus;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class CustomerTransformationService {

    public void transformCustomerName(TransformedCustomer transformedCustomer, String fullName, String customerType){
        if(customerType.equals(CustomerType.ORGANISATION.name())){
            transformedCustomer.setFirstName(fullName.toUpperCase());
            transformedCustomer.setLastName(null);
        }else {
            String[] nameSplitArray = fullName.trim().split(" ");
            int wordCount = nameSplitArray.length;
            transformedCustomer.setFirstName(nameSplitArray[0].toUpperCase());
            transformedCustomer.setLastName(wordCount > 1 ? nameSplitArray[wordCount - 1].toUpperCase() : null);
        }
    }

    public void transformLocation(TransformedCustomer transformedCustomer, String location){
        transformedCustomer.setCountry(LocationToCountryMap.countryMap.get(location));
    }

    public void transformMembershipStatus(TransformedCustomer transformedCustomer, LocalDate joiningDate){
        int yearDifference = LocalDate.now().getYear() - joiningDate.getYear();
        transformedCustomer.setMemberShipStatus(
                yearDifference >= 5 ? MembershipStatus.ULTIMATE.name() :
                yearDifference >= 3 ? MembershipStatus.PRIME.name() :
                yearDifference == 2 ? MembershipStatus.EXTENDED.name() :
                MembershipStatus.BASIC.name()
        );
    }
}
