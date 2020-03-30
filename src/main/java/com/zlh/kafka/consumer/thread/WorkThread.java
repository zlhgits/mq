package com.zlh.kafka.consumer.thread;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

/**
 *
 * Author zlh
 * Date 2018-07-17
 * Version 1.0
 */
public class WorkThread implements Callable<String> {
    private static Logger LOG = LogManager.getLogger(WorkThread.class);

    /**
     * 记录解析失败的日志或者发送到kafka集群失败的日志
     */
    private String topic;
    private String message;
    private Semaphore semaphore;//多副本并发访问控制类

    public WorkThread(String topic, String message, Semaphore semaphore){
        this.topic = topic;
        this.message = message;
        this.semaphore = semaphore;
    }

    public String call() throws Exception {
        try{
            //这里只打印下，如果在实际业务中处理失败，可能是代码bug或者系统不稳定等，先将消息记录到日志中，后续可以处理,不影响主流程的继续运行
            LOG.info("topic is {}, message is {}  "+ topic+"----"+ message);
        }catch (Exception e){
            LOG.error("ParseKafkaLogJob run error. ", e);
        }finally {

        }
        return "done";
    }
}
