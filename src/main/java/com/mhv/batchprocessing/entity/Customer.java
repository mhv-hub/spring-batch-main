package com.mhv.batchprocessing.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Customer {

    private long id;
    private String uniqueId;
    private String customerName;
    private String customerGender;
    private LocalDate customerDateOfBirth;
    private String customerType;
    private String location;
    private LocalDate joiningDate;
    private String activeStatus;

    public StringBuilder toStringCsvFormat(){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        String joiningDateString = this.joiningDate.format(formatter);
        String dateOfBirthString = this.joiningDate.format(formatter);
        return new StringBuilder()
                .append(this.uniqueId).append(",")
                .append(this.customerName).append(",")
                .append(this.customerGender).append(",")
                .append(dateOfBirthString).append(",")
                .append(this.customerType).append(",")
                .append(this.location).append(",")
                .append(joiningDateString).append(",")
                .append(this.activeStatus);
    }
}
