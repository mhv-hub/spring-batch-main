package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.reader;

import com.mhv.batchprocessing.entity.Customer;
import org.springframework.batch.core.StepListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class CustomerReaderCsv {

    @Bean(name = "customerCsvFileReader")
    @StepScope
    public FlatFileItemReader<Customer> itemReader(@Value("#{jobParameters[fileName]}") String fileName){
        FlatFileItemReader<Customer> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new ClassPathResource("customerData/" + fileName));
        flatFileItemReader.setName("csvFileReader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    @Bean
    private LineMapper<Customer> lineMapper() {
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("uniqueId", "customerName", "customerGender", "customerDateOfBirth", "customerType", "location", "joiningDate", "activeStatus");

        BeanWrapperFieldSetMapper<Customer> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        beanWrapperFieldSetMapper.setConversionService(dateConversionService());
        beanWrapperFieldSetMapper.setTargetType(Customer.class);

        DefaultLineMapper<Customer> customerDefaultLineMapper = new DefaultLineMapper<>();
        customerDefaultLineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);
        customerDefaultLineMapper.setLineTokenizer(lineTokenizer);
        return customerDefaultLineMapper;
    }

    @Bean
    public ConversionService dateConversionService() {
        DefaultConversionService conversionService = new DefaultConversionService();
        DefaultConversionService.addDefaultConverters(conversionService);
        conversionService.addConverter(new Converter<String, LocalDate>() {
            @Override
            public LocalDate convert(String dateAsString) {
                return LocalDate.parse(dateAsString.equals("NA") ? "01/01/9999" : dateAsString, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            }
        });
        return conversionService;
    }
}
