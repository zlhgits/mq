package com.zlh.kafka.producer.callback;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Author zlh
 * Date 2018-07-12
 * Version 1.0
 */
public class KafkaSend {
    private static final Logger LOG = LogManager.getLogger ( KafkaSend.class );
    private static KafkaSend ksp = null;
    private static KafkaProducer<String, String> producer;

    private KafkaSend() {
    }

    /**
     * 单例
     *
     * @return
     */
    public static KafkaSend getInstance() {
        if (ksp == null) {
            synchronized (KafkaSend.class) {
                if (ksp == null) {
                    ksp = new KafkaSend ();
                }
            }
        }
        return ksp;
    }

    /**
     * 配置初始化
     *
     * @param configFile
     */
    public void init(String configFile) throws Exception {
        try {
            Properties props = new Properties ();
            props.load ( new FileInputStream ( configFile ) );
            producer = new KafkaProducer<String, String> ( props );
        } catch (Exception e) {
            throw new Exception ( "初始化producer异常" + e.getMessage () );
        }
    }

    /**
     * 准备发送数据
     *
     * @param topic   主题
     * @param key     key
     * @param message 消息体
     * @param callNum 回调次数
     */
    public void send(String topic, String key, String message, int callNum) {
        ProducerRecord record;
        record = new ProducerRecord ( topic, key, message );
        ProducerCallback pcb = new ProducerCallback ( record, ksp, callNum );
        ksp.send ( record, pcb );
    }

    /**
     * 发送数据
     *
     * @param record
     * @param pcb
     */
    public void send(ProducerRecord record, ProducerCallback pcb) {
        try {
            producer.send ( record, pcb ).get ();
        } catch (InterruptedException e) {
            LOG.error ( "发送失败" + e.getMessage () );
        } catch (ExecutionException e) {
            LOG.error ( "发送失败" + e.getMessage () );
        }
        producer.close ();
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            KafkaSend kpa = KafkaSend.getInstance ();
            kpa.init ( "src/main/resources/kafkaproducer.properties" );

            //发送
            kpa.send ( "test6", null,
                    System.currentTimeMillis () + "stagetest1", 1 );
        } catch (Exception e) {
            e.printStackTrace ();
        }
    }
}
