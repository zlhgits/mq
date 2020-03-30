package com.zlh.kafka.consumer.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 手动确认offset
 * Author zlh
 * Date 2019-01-03
 * Version 1.0
 */
public class NonAutoCommit {
    private static Properties props;

    public static void main(String[] args) {
        props = new Properties();
        props.put("bootstrap.servers","192.168.1.11:9092,192.168.1.12:9092");
        props.put("group.id","grouptest1");
        props.put("enable.auto.commit","false");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //从最早偏移量开始消费
        props.setProperty("auto.offset.reset","earliest");
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅topics
        consumer.subscribe( Arrays.asList("test6"));
        final int minBatchSize = 200;

        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>> ();
        consumer.seek(new TopicPartition ("test6",0),0);
        while(true){
            //poll参数超时设置
            ConsumerRecords<String, String> recordes = consumer.poll(1000);
            for (ConsumerRecord<String, String> recorde : recordes) {

                System.out.println("key:  " + recorde.key()+" value:  " + recorde.value());
                System.out.println(" group:  " + props.getProperty("group.id")+" topic:  " + recorde.topic()+" partition:  " + recorde.partition()+ " offset:  " + recorde.offset());
                buffer.add(recorde);
            }

            if(buffer.size() >= minBatchSize ) {
                System.out.println(buffer.get(0).toString());
                //手动提交
                consumer.commitAsync();
                buffer.clear();
            }
        }
    }
}
