package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.step;

import com.mhv.batchprocessing.entity.Customer;
import com.mhv.batchprocessing.entity.TransformedCustomer;
import com.mhv.batchprocessing.exceptionHandeler.GeneralException;
import org.hibernate.HibernateException;
import org.hibernate.exception.DataException;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class CustomerTransformationLoadStep {

    @Autowired
    @Qualifier(value = "customerKafkaQueueReaderBean")
    private KafkaItemReader<String, Customer> itemReader;

    @Autowired
    @Qualifier(value = "customerTransformationProcessorBean")
    private ItemProcessor<Customer, TransformedCustomer> itemProcessor;

    @Autowired
    @Qualifier(value = "customerDataWriterBean")
    private ItemWriter<TransformedCustomer> itemWriter;

    @Autowired
    @Qualifier(value = "transformationAndLoadSkipListenerBean")
    private SkipListener<Customer, TransformedCustomer> skipListener;

    @Autowired
    @Qualifier(value = "transformationProcessListenerBean")
    private ItemProcessListener<Customer, TransformedCustomer> itemProcessListener;

    @Autowired
    @Qualifier(value = "customerWriteListenerBean")
    private ItemWriteListener<TransformedCustomer> customerItemWriteListener;

    @Bean(name = "customerTransformationLoadStepBean")
    public Step getCustomerTransformationLoadStep(JobRepository jobRepository, PlatformTransactionManager transactionManager){
        return new StepBuilder("customer-transform-and-load-step", jobRepository)
                .<Customer, TransformedCustomer>chunk(100, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .faultTolerant()
                .skipLimit(Integer.MAX_VALUE)
                .skip(DataException.class)
                .skip(DataIntegrityViolationException.class)
                .skip(GeneralException.class)
                .skip(HibernateException.class)
                .listener(skipListener)
                .listener(itemProcessListener)
                .listener(customerItemWriteListener)
                .build();
    }
}
