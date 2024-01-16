package com.mhv.batchprocessing.entity;

import com.mhv.batchprocessing.util.LocationToCountryMap;
import com.mhv.batchprocessing.util.MembershipStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDate;

@Entity
@Table(name = "CUSTOMER_INFO")
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TransformedCustomer {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "CUSTOMER_ID")
    private long id;

    @Column(name = "UNIQUE_ID", length = 10, unique = true)
    private String uniqueId;

    @Column(name = "FIRST_NAME", length = 75)
    private String firstName;

    @Column(name = "LAST_NAME", length = 25)
    private String lastName;

    @Column(name = "GENDER", length = 6)
    private String gender;

    @Column(name = "DOB")
    private LocalDate dateOfBirth;

    @Column(name = "CATEGORY", length = 30)
    private String category;

    @Column(name = "COUNTRY", length = 25)
    private String country;

    @Column(name = "JOINING_DATE")
    private LocalDate joiningDate;

    @Column(name = "ACTIVE_STATUS", length = 10)
    private String activeStatus;

    @Column(name = "MEMBERSHIP", length = 10)
    private String memberShipStatus;

    public TransformedCustomer(Customer customer){
        this.setUniqueId(customer.getUniqueId());
        this.setGender(customer.getCustomerGender());
        this.setDateOfBirth(customer.getCustomerDateOfBirth());
        this.setCategory(customer.getCustomerType());
        this.setJoiningDate(customer.getJoiningDate());
        this.setActiveStatus(customer.getActiveStatus());
    }
}
