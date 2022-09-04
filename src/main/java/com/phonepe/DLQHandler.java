package com.phonepe;

import com.phonepe.api.MessageQueue;
import com.phonepe.api.ProducerErrorHandler;
import com.phonepe.api.Record;

public class DLQHandler implements ProducerErrorHandler {

    private MessageQueue dlQueue;

    @Override
    public void handleError(Record record) {
        try {
            dlQueue.notify(record);
        } catch (Exception ex) {

        }
    }
}
