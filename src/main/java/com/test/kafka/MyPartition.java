package com.test.kafka;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 *
 （1）如果指定了某个分区,会只将消息发到这个分区上

 （2）如果同时指定了某个分区和key,则也会将消息发送到指定分区上,key不起作用

 （3）如果没有指定分区和key,那么将会随机发送到topic的分区中

 （4）如果指定了key,那么将会以hash<key>的方式发送到分区中
 */
public class MyPartition implements Partitioner {

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int partitionNum = 0;
        try {
            partitionNum = Integer.parseInt((String) key);
        } catch (Exception e) {
            partitionNum = key.hashCode();
        }
        return Math.abs(partitionNum % numPartitions);
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
