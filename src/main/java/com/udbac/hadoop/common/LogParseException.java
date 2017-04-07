package com.udbac.hadoop.common;

/**
 * Created by root on 2017/3/24.
 */
public class LogParseException extends Exception{
    public LogParseException() {
    }

    public LogParseException(String message) {
        super(message);
    }

    public LogParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
