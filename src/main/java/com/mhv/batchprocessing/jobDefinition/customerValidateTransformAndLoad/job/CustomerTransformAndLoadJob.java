package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CustomerTransformAndLoadJob {

    @Autowired
    @Qualifier(value = "customerTransformationLoadStepBean")
    private Step step;

    @Bean(name = "customerTransformAndLoadJobBean")
    public Job getCustomerTransformAndLoadJob(JobRepository jobRepository){
        return new JobBuilder("customer-transform-and-load-job", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(step)
                .end()
                .build();
    }
}
