package com.bin.test.rabbitmqtest.ack;

/**
 * Created by joy on 9/1/16.
 */
public class TestConfig {

    private int queuesCount;
    private int batchSize;
    private int interval;
    private int ticksBias;

    public int getQueuesCount() {
        return queuesCount;
    }

    public void setQueuesCount(int queuesCount) {
        this.queuesCount = queuesCount;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public int getTicksBias() {
        return ticksBias;
    }

    public void setTicksBias(int ticksBias) {
        this.ticksBias = ticksBias;
    }
}
