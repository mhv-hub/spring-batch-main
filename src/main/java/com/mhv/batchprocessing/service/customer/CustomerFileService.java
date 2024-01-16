package com.mhv.batchprocessing.service.customer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mhv.batchprocessing.dto.RejectedRecord;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Service
public class CustomerFileService {


    public List<RejectedRecord> getRejectedRecordList(String jobKey) throws IOException {
        String rejectFileName = "customerRejectFile_" + jobKey + ".csv";
        String fileLocation = new ClassPathResource("customerRejectData/").getFile().getAbsolutePath();
        String rejectFile = fileLocation + File.separator + rejectFileName;
        File file = new File(rejectFile);
        List<RejectedRecord> rejectedRecordList = new ArrayList<>();
        if(!file.exists()) return rejectedRecordList;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            bufferedReader.lines().forEach(line -> {
                try {
                    rejectedRecordList.add(new ObjectMapper().readValue(line, RejectedRecord.class));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Record in reject file corrupted [ Record : " + line + " ]");
                }
            });
        }catch (Exception e){
            throw new RuntimeException("Failed to read  rejected records [ Filename : " + rejectFile + " ]");
        }
        return rejectedRecordList;
    }
}
