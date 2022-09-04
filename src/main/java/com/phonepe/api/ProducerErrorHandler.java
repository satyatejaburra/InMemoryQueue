package com.phonepe.api;

import com.phonepe.QueueOverflowException;

public interface ProducerErrorHandler {

    void handleError(Record record) throws QueueOverflowException;
}
