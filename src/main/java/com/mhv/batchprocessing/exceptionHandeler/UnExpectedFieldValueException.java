package com.mhv.batchprocessing.exceptionHandeler;

public class UnExpectedFieldValueException extends Exception{

    public UnExpectedFieldValueException(String message){
        super(message);
    }
}
