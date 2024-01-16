package com.mhv.batchprocessing.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class JobStatusResponse {

    private long jobKey;
    private String startTime;
    private String endTime;
    private String duration;
    private String jobStatus;
    private String exitMessage;
    private long totalRecordCount;
    private long processedRecordCount;
    private long rejectedRecordCount;
    private List<RejectedRecord> rejectedRecords;
    private int responseCode;
}
