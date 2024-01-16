package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mhv.batchprocessing.dto.RejectedRecord;
import com.mhv.batchprocessing.entity.Customer;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Component
@Qualifier(value = "validationSkipListenerBean")
@StepScope
@SuppressWarnings({"ConstantConditions"})
public class ValidationSkipListener implements SkipListener<Customer, Customer> {

    @Value("#{stepExecution.jobExecution}")
    private JobExecution jobExecution;

    private final BufferedWriter bufferedWriter;
    private String rejectFile;

    public ValidationSkipListener(@Value("#{jobParameters[jobKey]}") String jobKey) throws IOException {
        String rejectFileName = "customerRejectFile_" + jobKey + ".csv";
        String fileLocation = new ClassPathResource("customerRejectData/").getFile().getAbsolutePath();
        String rejectFile = fileLocation + File.separator + rejectFileName;
        this.bufferedWriter = new BufferedWriter(new FileWriter(rejectFile, true));
    }

    @Override
    public void onSkipInRead(Throwable t) {
        System.out.println(">>>>>>>>>>>>>>> [ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Error in CSV record read. Record skipped [ Error : " + t.getClass().getName() + " - " + t.getMessage() + " ]");
        if(t instanceof FlatFileParseException){
            writeRejectedRecordToFile(new RejectedRecord("Failed to read the record due to malformed structure", ((FlatFileParseException) t).getInput()));
        }
    }

    @Override
    public void onSkipInWrite(Customer customer, Throwable t) {
        System.out.println(">>>>>>>>>>>>>>> [ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Error in Kafka write. Record skipped [ Error : " + t.getClass().getName() + " - " + t.getMessage() + " ]");
        writeRejectedRecordToFile(new RejectedRecord(t.getMessage(), customer.toStringCsvFormat().toString()));
    }

    @Override
    public void onSkipInProcess(Customer customer, Throwable t) {
        System.out.println(">>>>>>>>>>>>>>> [ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Error in customer validation. Record skipped [ Error : " + t.getClass().getName() + " - " + t.getMessage() + " ]");
        writeRejectedRecordToFile(new RejectedRecord(t.getMessage(), customer.toStringCsvFormat().toString()));
    }

    private void writeRejectedRecordToFile(RejectedRecord record) {
        try {
            String rejectedRecordJson = new ObjectMapper().writeValueAsString(record);
            System.out.println("[ Thread # " + Thread.currentThread().getName() + " ] =============== [ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Writing rejected record in file [ record : " + rejectedRecordJson + " ]");
            bufferedWriter.write(rejectedRecordJson);
            bufferedWriter.newLine();
            bufferedWriter.flush();
        }catch (Exception e){
            throw new RuntimeException("[ Thread # " + Thread.currentThread().getName() + " ] =============== [ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] Failed to write rejected record to file [ FileName : " + this.rejectFile);
        }
    }
}
