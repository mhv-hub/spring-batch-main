package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CustomerValidatorJob {

    @Autowired
    @Qualifier(value = "customerCsvProcessorStepBean")
    private Step processCsvStep;

    @Autowired
    @Qualifier(value = "deleteCsvFileStepBean")
    private Step deleteCsvStep;

    @Autowired
    @Qualifier(value = "csvProcessorJobListenerBean")
    private JobExecutionListener jobExecutionListener;

    @Bean(name = "customerValidatorJobBean")
    public Job getCustomerValidatorJob(JobRepository jobRepository){
        return new JobBuilder("customer-validator-job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(processCsvStep)
                .next(deleteCsvStep)
                .end()
                .listener(jobExecutionListener)
                .build();
    }
}
