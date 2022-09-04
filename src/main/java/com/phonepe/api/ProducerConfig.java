package com.phonepe.api;

public class ProducerConfig {

    private boolean enableDLQ;

    public ProducerConfig(boolean enableDLQ) {
        this.enableDLQ = enableDLQ;
    }

    public boolean isEnableDLQ() {
        return enableDLQ;
    }

    public void setEnableDLQ(boolean enableDLQ) {
        this.enableDLQ = enableDLQ;
    }
}
