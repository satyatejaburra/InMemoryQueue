package com.phonepe;

import com.phonepe.api.MessageQueue;
import com.phonepe.api.Producer;
import com.phonepe.api.ProducerConfig;
import com.phonepe.api.Record;

public class JSONProducer implements Producer {

    private final MessageQueue messageQueue;

    private  DLQHandler dlqHandler;

    private final ProducerConfig producerConfig;

    public JSONProducer(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
        this.producerConfig = new ProducerConfig(true);
    }

    public JSONProducer(MessageQueue messageQueue, ProducerConfig producerConfig) {
        this.messageQueue = messageQueue;
        this.producerConfig = producerConfig;
    }

    @Override
    public void produce(Record record) {
        while (true) {
            try {
                messageQueue.notify(record);
                break;
            } catch (QueueOverflowException e) {
                if (producerConfig.isEnableDLQ()) {
                    dlqHandler.handleError(record);
                }
            }
        }
    }
}
