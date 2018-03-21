package com.test.kafka.multithread.singleconsumer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class NotificationProducerThread implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public NotificationProducerThread(String broker, String topic) {
        Properties properties = createProducerConfig(broker);
        this.producer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
    }

    private static Properties createProducerConfig(String broker) {
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("bootstrap.servers", broker);
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("batch.size", 16384);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    public void run() {
        System.out.println("produces 10 messages");
        for (int i = 0; i < 100; i++) {
            final String message = "message " + i;
            producer.send(new ProducerRecord<String, String>(topic, message), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                    System.out.println("Sent: " + message + ", partition: " + metadata.partition() + ", offset: " + metadata.offset() );
                }
            });
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
