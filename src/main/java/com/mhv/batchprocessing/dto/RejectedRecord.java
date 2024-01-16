package com.mhv.batchprocessing.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class RejectedRecord implements Serializable {

    private String errorMessage;
    private String record;
}
