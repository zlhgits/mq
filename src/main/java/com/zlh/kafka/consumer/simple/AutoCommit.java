package com.zlh.kafka.consumer.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 自动确认offset
 * Author zlh
 * Date 2018-08-16
 * Version 1.0
 */
public class AutoCommit {
    public static void main(String[] args) {
        new AutoCommit().kafkaCum();
    }

    public void kafkaCum() {
        Properties props = new Properties();
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", "cdh-master:9092,cdh-node1:9092,cdh-node2:9092");
        /* 制定consumer group */
        props.put("group.id", "test1");
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* 定义consumer */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        /* 消费者订阅的topic, 可同时订阅多个 */
        consumer.subscribe( Arrays.asList("sys_unit"));

        /* 读取数据，读取超时时间为1000ms */
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records)
                System.out.println("offset = %d, key = %s, value = %s"+ record.offset()+"--s"+record.key()+"--"+record.value());
        }
    }
}
