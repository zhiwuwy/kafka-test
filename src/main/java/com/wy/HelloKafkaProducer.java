package com.wy;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author wangyang
 * @date 2020/9/5 13:27
 * @description:
 */
public class HelloKafkaProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        //只需要写一个，就可以找到其他broker，但是，为了避免宕机，还是写两个比较好
        properties.setProperty("bootstrap.servers","192.168.198.100:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>("hello-topic","student1","value2");

        try{
            //异步阻塞方式
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();
            System.out.println(recordMetadata);
            //异步非阻塞方式
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(recordMetadata + "======================");
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
