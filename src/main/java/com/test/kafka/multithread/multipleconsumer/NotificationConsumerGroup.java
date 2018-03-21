package com.test.kafka.multithread.multipleconsumer;

import java.util.ArrayList;
import java.util.List;

public class NotificationConsumerGroup {

    private final int numberOfConsumers;
    private final String broker;
    private final String groupId;
    private final String topic;
    private List<NotificationConsumerThread> consumers;

    public NotificationConsumerGroup(int numberOfConsumers, String broker, String groupId, String topic) {
        this.numberOfConsumers = numberOfConsumers;
        this.broker = broker;
        this.groupId = groupId;
        this.topic = topic;
        consumers = new ArrayList<NotificationConsumerThread>();
        for(int i = 0; i < numberOfConsumers; i++) {
            consumers.add(new NotificationConsumerThread(this.broker, this.groupId, this.topic));
        }
    }

    public void execute() {
        for (NotificationConsumerThread thread: consumers) {
            Thread t = new Thread(thread);
            t.start();
        }
    }

    public int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    public String getGroupId() {
        return groupId;
    }

}
