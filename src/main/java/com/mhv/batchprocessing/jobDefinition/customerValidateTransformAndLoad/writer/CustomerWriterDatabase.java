package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.writer;

import com.mhv.batchprocessing.entity.TransformedCustomer;
import com.mhv.batchprocessing.repository.CustomerRepository;
import jakarta.persistence.EntityManager;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CustomerWriterDatabase {

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private EntityManager entityManager;

    @Bean(name = "customerDataWriterBean")
    public JpaItemWriter<TransformedCustomer> itemWriter(){
        JpaItemWriter<TransformedCustomer> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManager.getEntityManagerFactory());
        return jpaItemWriter;
    }
}
