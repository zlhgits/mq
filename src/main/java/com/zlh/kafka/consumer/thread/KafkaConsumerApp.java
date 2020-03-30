package com.zlh.kafka.consumer.thread;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 * Author zlh
 * Date 2018-07-17
 * Version 1.0
 */
public class KafkaConsumerApp {
    private static final Logger LOG = LogManager.getLogger(KafkaConsumerApp.class);

    /**
     * 用于接收kafka 消息的线程池
     */
    private ExecutorService kafkaConsumerExecutorService;
    /**
     * 具体处理kafka消息的线程池
     */
    private ExecutorService workerExecutorService;
    /**
     * 用于阻塞往线程池中提交新的任务，直到有可用的线程
     */
    private Semaphore semaphore;
    private int kafkaConsumerExecutorNumber = 10;
    private int workerExecutorNumber = 50;

    public KafkaConsumerApp(){
        //线程池的大小可以根据自己需要来调节，这里简单点就使用了固定线程池
        //用于接收kafka 消息的线程池
        kafkaConsumerExecutorService = Executors.newFixedThreadPool(kafkaConsumerExecutorNumber);
        //具体处理kafka消息的线程池
        workerExecutorService = Executors.newFixedThreadPool(workerExecutorNumber);
        //用于阻塞往线程池中提交新的任务，直到有可用的线程
        semaphore = new Semaphore(workerExecutorNumber);
    }

    private void start() {
        for(int i = 0; i < kafkaConsumerExecutorNumber; i++){
            kafkaConsumerExecutorService.submit(new TopicPartitionThread(workerExecutorService, semaphore));
        }
    }

    private void destroy() throws Exception {
        //停止kafka 消费线程
        StautsCache.getInstance().setKafkaThreadStatus(false);
        //关闭线程池
        kafkaConsumerExecutorService.shutdown();
        while(!kafkaConsumerExecutorService.awaitTermination(1, TimeUnit.SECONDS)){
            LOG.info("await kafkaConsumerExecutorService stop...");
        }
        LOG.info("kafkaConsumerExecutorService stoped.");
        workerExecutorService.shutdown();
        while(!workerExecutorService.awaitTermination(1, TimeUnit.SECONDS)){
            LOG.info("await workerExecutorService stop...");
        }
        LOG.info("workerExecutorService stoped.");
    }

    public static void main(String[] args) throws Exception {
        KafkaConsumerApp kca = new KafkaConsumerApp();
        //启动kafka消费
        kca.start();
        //运行一段时间后停止kafka消息接收
        Thread.sleep(3600 * 1000); //运行一个小时
        //关闭线程池，实际部署在生产上的应用，不要直接kill -9 强制关闭，请使用kill ，给应用关闭前做一些清理操作
        kca.destroy();

    }

}
