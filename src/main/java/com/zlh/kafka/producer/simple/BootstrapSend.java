package com.zlh.kafka.producer.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * 采用bootstrap.servers模式
 * Author zlh
 * Date 2019-01-03
 * Version 1.0
 */
public class BootstrapSend {
    public static void producerSend() throws InterruptedException {
        Properties props = new Properties();
        /**
         * metadata.broker.list是旧版本命令，这里的bootstrap.servers是新版命令
         */
        props.put("bootstrap.servers", "192.168.50.99:9092");
//        props.put("bootstrap.servers", "192.168.1.11:9092,192.168.1.12:9092");
//        props.put("bootstrap.servers", "122.224.117.42:39092,122.224.117.42:39093");
        /**
         * 消息存储是是否需要应答
         * acks: [1 - leader 应答
         * ,0,-1  无应答
         * all  follower -> leader 应答
         */
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        // 指定key value的序列化方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 2; i++) {
            String uuid = UUID.randomUUID().toString();
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("test111", uuid,"hello kafka " + uuid);
            //发送
            producer.send(record);
            Thread.sleep(500);
        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        producerSend();
    }
}
