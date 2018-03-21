package com.test.kafka.multithread.multipleconsumer;

public class MultipleConsumersMain {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String groupId = "test-group";
        String topic = "topic";
        int numberOfConsumer = 3;

        if (args != null && args.length > 4) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
            numberOfConsumer = Integer.parseInt(args[3]);
        }

        NotificationProducerThread producer = new NotificationProducerThread(brokers, topic);
        Thread p = new Thread(producer);
        p.start();

        NotificationConsumerGroup consumer = new NotificationConsumerGroup(numberOfConsumer, brokers, groupId, topic);
        consumer.execute();

        try {
            Thread.sleep(100000);
        } catch (Exception e) {
            e.printStackTrace();
        }



    }
}
