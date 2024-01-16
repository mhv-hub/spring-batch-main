package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.mhv.batchprocessing.dto.RejectedRecord;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

@Component
@Qualifier("csvProcessorJobListenerBean")
@JobScope
public class CsvProcessorJobListener implements JobExecutionListener {

    @Autowired
    @Qualifier(value = "validationSkipListenerBean")
    private ValidationSkipListener skipListener;

    @Override
    public void beforeJob(JobExecution jobExecution) {}

    @Override
    public void afterJob(JobExecution jobExecution) {
        StepExecution stepExecution = jobExecution.getStepExecutions().stream().filter(value -> value.getStepName().equals("customer-csv-processor")).findFirst().get();
        long totalRecordCount = stepExecution.getReadCount() + stepExecution.getReadSkipCount();
        long rejectedRecordCount = stepExecution.getReadSkipCount() + stepExecution.getProcessSkipCount() + stepExecution.getWriteSkipCount();
        long processedRecordCount = totalRecordCount - rejectedRecordCount;
        jobExecution.getExecutionContext().put("totalRecordCount", totalRecordCount);
        jobExecution.getExecutionContext().put("processedRecordCount", processedRecordCount);
        jobExecution.getExecutionContext().put("rejectedRecordCount", rejectedRecordCount);
        jobExecution.setExitStatus(stepExecution.getExitStatus());
    }
}
