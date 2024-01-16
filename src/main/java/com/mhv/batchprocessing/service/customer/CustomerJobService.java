package com.mhv.batchprocessing.service.customer;

import com.mhv.batchprocessing.dto.JobStatusResponse;
import com.mhv.batchprocessing.dto.RejectedRecord;
import com.mhv.batchprocessing.util.Helper;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
@SuppressWarnings({"ConstantConditions", "unchecked", "StatementWithEmptyBody"})
public class CustomerJobService {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier(value = "customerValidatorJobBean")
    private Job customerValidatorJob;

    @Autowired
    @Qualifier(value = "customerTransformAndLoadJobBean")
    private Job customerTransformationAndLoadJob;

    @Autowired
    private CustomerFileService customerFileService;

    public JobStatusResponse triggerCustomerValidationJob(String fileName, String jobKey) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException, IOException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLocalDateTime("startTime", LocalDateTime.now());
        jobParametersBuilder.addString("fileName", fileName);
        jobParametersBuilder.addLong("jobKey", Long.parseLong(jobKey));
        JobParameters jobParameters = jobParametersBuilder.toJobParameters();
        JobExecution jobExecution = jobLauncher.run(customerValidatorJob, jobParameters);
        while (jobExecution.isRunning()){}
        ExitStatus exitStatus = jobExecution.getExitStatus();
        LocalDateTime startTime = jobExecution.getStartTime();
        LocalDateTime endTime = jobExecution.getEndTime();
        String duration = Helper.getDateTimeDifference(startTime, endTime);
        long totalRecordCount = (long)jobExecution.getExecutionContext().get("totalRecordCount");
        long processedRecordCount = (long)jobExecution.getExecutionContext().get("processedRecordCount");
        long rejectedRecordCount = (long)jobExecution.getExecutionContext().get("rejectedRecordCount");
        List<RejectedRecord> rejectedRecords = customerFileService.getRejectedRecordList(jobKey);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss:SSS");
        System.out.println("[ KEY : " + jobKey + " ] Validation job " + exitStatus.getExitCode() + " <<<<<<<<<<<<<<<<<<<<<<<");
        return new JobStatusResponse(Long.parseLong(jobKey), startTime.format(dateTimeFormatter), endTime.format(dateTimeFormatter), duration, exitStatus.getExitCode(), exitStatus.getExitDescription(), totalRecordCount, processedRecordCount, rejectedRecordCount, rejectedRecords, exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode()) ? 500 : 200);
    }

    public void triggerCustomerTransformationAndLoadJob(String key) throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addLocalDateTime("startTime", LocalDateTime.now());
        jobParametersBuilder.addString("jobKey", key);
        JobParameters jobParameters = jobParametersBuilder.toJobParameters();
        JobExecution jobExecution = jobLauncher.run(customerTransformationAndLoadJob, jobParameters);
        while (jobExecution.isRunning()){}
        System.out.println("Transformation and Load Job Status : [ " + jobExecution.getStatus() + " ]");
    }

    public String getNewJobKey() {
        return String.valueOf(new Random().nextInt(100) + 101);
    }
}
