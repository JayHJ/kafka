package com.test.kafka.singlethread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {

    private static KafkaConsumer createConsumerConfig(String z_zookeeper, String a_groupId) {
        z_zookeeper = "localhost:2181";
        a_groupId = "test_group";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect", z_zookeeper);
        properties.put("group.id", a_groupId);
        properties.put("zookeeper.session.timeout.ms", "40000");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(properties);
    }

    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = createConsumerConfig("", "");
        kafkaConsumer.subscribe(Arrays.asList("my-topic", "test-topic"));

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("收到消息：" + record.value() + "Topic:" + record.topic());
                System.out.println(record.partition());
            }
        }

    }
}
