package com.wy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.*;

/**
 * @author wangyang
 * @date 2020/9/5 13:47
 * @description:
 */
public class HelloKafkaConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.198.100:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Collections.singletonList("hello-topic"));
        while (true){
            ConsumerRecords<String,String> poll = kafkaConsumer.poll(500);
            for (ConsumerRecord<String, String> stringStringConsumerRecord : poll) {
                System.out.println(String.format("topic:%s,分区:%d,偏移量:%d,key:%s,value:%s",stringStringConsumerRecord.topic(),
                        stringStringConsumerRecord.partition(),stringStringConsumerRecord.offset()
                        ,stringStringConsumerRecord.key(),stringStringConsumerRecord.value()));
            }

        }
    }
}
