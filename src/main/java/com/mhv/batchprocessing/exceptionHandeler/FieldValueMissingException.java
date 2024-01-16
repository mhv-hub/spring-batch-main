package com.mhv.batchprocessing.exceptionHandeler;

public class FieldValueMissingException extends Exception{

    public FieldValueMissingException(String message){
        super(message);
    }
}
