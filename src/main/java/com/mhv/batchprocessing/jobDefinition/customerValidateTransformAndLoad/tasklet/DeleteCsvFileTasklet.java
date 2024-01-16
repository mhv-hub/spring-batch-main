package com.mhv.batchprocessing.jobDefinition.customerValidateTransformAndLoad.tasklet;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
@Qualifier(value = "deleteCsvFileTaskletBean")
@StepScope
public class DeleteCsvFileTasklet implements Tasklet {

    @Value("#{stepExecution.jobExecution}")
    private JobExecution jobExecution;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String fileName = chunkContext.getStepContext().getJobParameters().get("fileName").toString();
        try{
            runCustomMethod(fileName);
            System.out.println("[ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] File deleted : " + fileName);
        }catch (IOException e){
            System.out.println("[ KEY : " + jobExecution.getJobParameters().getLong("jobKey") + " ] File deletion failed : " + e.getMessage());
        }
        return RepeatStatus.FINISHED;
    }

    private void runCustomMethod(String fileName) throws IOException {
        File fileLocation = new ClassPathResource("customerData/").getFile();
        Path path = Paths.get(fileLocation.getAbsolutePath() + File.separator + fileName);
        Files.delete(path);
    }
}
