package com.test.kafka.multithread.multipleconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class NotificationConsumerThread implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public NotificationConsumerThread(String broker, String groupId, String topic) {
        Properties prop = createConsumerConfig(broker, groupId);
        this.consumer = new KafkaConsumer<String, String>(prop);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void run() {
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record.value() + ", partition: " + record.partition() +
                ", offset: " + record.offset() + ", by ThreadID: " + Thread.currentThread().getId());
            }
        }
    }
}
