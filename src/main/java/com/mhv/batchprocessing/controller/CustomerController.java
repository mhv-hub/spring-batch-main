package com.mhv.batchprocessing.controller;

import com.mhv.batchprocessing.dto.JobStatusResponse;
import com.mhv.batchprocessing.exceptionHandeler.GeneralException;
import com.mhv.batchprocessing.service.customer.CustomerJobService;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
public class CustomerController {

    @Autowired
    private CustomerJobService customerJobService;

    @PostMapping("/api/customer/process-and-load")
    public ResponseEntity<JobStatusResponse> validateInputFileAndTriggerCustomerService(@RequestParam("csv-customer-data") MultipartFile multipartFile) throws IOException, MultipartException, GeneralException, JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        if(multipartFile == null || multipartFile.getOriginalFilename() == null){
            throw new GeneralException("Invalid or no file provided");
        }
        String fileName = multipartFile.getOriginalFilename();
        String key = customerJobService.getNewJobKey();
        if(!fileName.endsWith(".csv")){
            throw new GeneralException("Please upload a valid CSV file");
        }else{
            fileName = "customerDataFile_" + key + ".csv";
        }
        File filLocation = new ClassPathResource("customerData/").getFile();
        Path path = Paths.get(filLocation.getAbsolutePath() + File.separator + fileName);
        Files.copy(multipartFile.getInputStream(), path, StandardCopyOption.REPLACE_EXISTING);
        JobStatusResponse jobStatusResponse = customerJobService.triggerCustomerValidationJob(fileName, key);
        return ResponseEntity.status(jobStatusResponse.getResponseCode()).body(jobStatusResponse);
    }
}
