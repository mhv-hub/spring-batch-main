package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.step;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.exceptionHandeler.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class CustomerCsvProcessorStep {

    @Autowired
    @Qualifier(value = "customerCsvFileReader")
    private ItemReader<Customer> itemReader;

    @Autowired
    @Qualifier(value = "customerMessageEventWriter")
    private ItemWriter<Customer> itemWriter;

    @Autowired
    @Qualifier(value = "customerCsvDataProcessor")
    private ItemProcessor<Customer, Customer> itemProcessor;

    @Autowired
    @Qualifier(value = "fiveThreadsTaskExecutorBean")
    private TaskExecutor taskExecutor;

    @Autowired
    @Qualifier(value = "validationSkipListenerBean")
    private SkipListener<Customer, Customer> skipListener;

    @Autowired
    @Qualifier(value = "customerValidationProcessListenerBean")
    private ItemProcessListener<Customer, Customer> itemProcessListener;

    @Autowired
    @Qualifier(value = "customerKafkaWriteListenerBean")
    private ItemWriteListener<Customer> itemWriteListener;

    @Autowired
    @Qualifier(value = "validationStepListenerBean")
    private StepExecutionListener stepExecutionListener;

    @Bean(name = "customerCsvProcessorStepBean")
    public Step getCustomerCsvProcessorStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("customer-csv-processor", jobRepository)
                .<Customer, Customer>chunk(100, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .skip(UnExpectedFieldValueException.class)
                .skip(FieldValueBlankException.class)
                .skip(FieldLengthOutOfRangeException.class)
                .skip(FieldValueMissingException.class)
                .skip(IllegalFieldValueException.class)
                .skipLimit(Integer.MAX_VALUE)
                .listener(skipListener)
                .listener(itemProcessListener)
                .listener(itemWriteListener)
                .taskExecutor(taskExecutor)
                .listener(stepExecutionListener)
                .build();
    }
}
