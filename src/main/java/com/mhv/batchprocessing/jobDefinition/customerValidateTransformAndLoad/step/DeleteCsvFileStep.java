package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.step;

import com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.tasklet.DeleteCsvFileTasklet;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class DeleteCsvFileStep {

    @Autowired
    @Qualifier(value = "deleteCsvFileTaskletBean")
    private DeleteCsvFileTasklet deleteCsvFileTasklet;

    @Bean(name = "deleteCsvFileStepBean")
    public Step getDeleteCsvFileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("delete-csv-file", jobRepository)
                .tasklet(deleteCsvFileTasklet, transactionManager)
                .build();
    }
}
