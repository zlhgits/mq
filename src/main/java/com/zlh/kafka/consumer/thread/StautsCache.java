package com.zlh.kafka.consumer.thread;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * Author zlh
 * Date 2018-07-18
 * Version 1.0
 */
public class StautsCache {
    private static final Logger LOG = LogManager.getLogger(StautsCache.class);

    private static StautsCache sc = null;
    /**
     * kafka 拉取数据线程的状态， false：停止获取数据
     */
    private boolean kafkaThreadStatus = true;

    public StautsCache(){}

    /**
     * 单例
     * @return
     */
    public static StautsCache getInstance(){
        if (sc == null){
            synchronized(KafkaConsumerApp.class){
                if (sc == null){
                    sc = new StautsCache();
                }
            }
        }
        return sc;
    }

    public boolean isKafkaThreadStatus() {
        return kafkaThreadStatus;
    }
    public void setKafkaThreadStatus(boolean kafkaThreadStatus) {
        this.kafkaThreadStatus = kafkaThreadStatus;
    }

}
