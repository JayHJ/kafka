package com.test.kafka.multithread.singleconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerThreadHandler implements Runnable {
    private final ConsumerRecord<String, String> consumerRecord;

    public ConsumerThreadHandler(ConsumerRecord<String, String> consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public void run() {
        System.out.println("Process: " + consumerRecord.value() + ", Offset: " + consumerRecord.offset()
                + ", By ThreadID: " + Thread.currentThread().getId());
    }
}
