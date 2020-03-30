package com.zlh.kafka.producer.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 数据发送状态处理
 * Author zlh
 * Date 2018-07-12
 * Version 1.0
 */
public class ProducerCallback implements Callback {
    private static final Logger LOG = LogManager.getLogger(ProducerCallback.class);
    private ProducerRecord<String,String> record;
    private KafkaSend ksp;
    private int callNum = 0;

    public ProducerCallback(ProducerRecord record, KafkaSend ksp,int callNum){
        this.record = record;
        this.ksp = ksp;
    }

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e == null){
            String str = "发送成功："+ record.toString() + "topic:" + recordMetadata.topic() + ", partition:"
                    + recordMetadata.partition() + ", offset:" + recordMetadata.offset();
            LOG.info(str);
            System.out.println(str);
            return;
        }
        String str = "发送失败， record:" + record.toString() + ", errmsg:" + e.getMessage();
        //record:ProducerRecord(topic=stagetest1, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=stagetest14, value=stagetest1b4444444bb3, timestamp=null), errmsg:Failed to update metadata after 60000 ms.
        LOG.error(str);
        System.out.println(str);
        //重试
        if (callNum > 0){
            //回调
            ProducerCallback pcb = new ProducerCallback(record, ksp,--callNum);
            ksp.send(record,pcb);
        }else{
            // 发送失败，转存到临时表(192.168.1.5@zndata.stage_kafka_failedmsg)
            /**
             * String topic = record.topic();
             * String key = record.key();
             * String value = record.value();
             * String errmsg = e.getMessage();
             */
//        Boolean flag = DBUtil.insertIntoTable(record,e);
        }
    }
}
