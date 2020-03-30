package com.zlh.kafka.consumer.thread;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 *
 * Author zlh
 * Date 2018-07-17
 * Version 1.0
 */
public class TopicPartitionThread extends Thread {
    private static Logger LOG = LogManager.getLogger(TopicPartitionThread.class);
    private ExecutorService workExecutorService;
    private Semaphore semaphore;
    private KafkaConsumer<String, String> consumer;

    private Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<TopicPartition, OffsetAndMetadata>();

    private List<Future<String>> taskList = new ArrayList<Future<String>>();

    public TopicPartitionThread(){}
    public TopicPartitionThread(ExecutorService workExecutorService, Semaphore semaphore){
        this.workExecutorService = workExecutorService;
        this.semaphore = semaphore;
    }

    /**
     * 配置初始化
     * @param configFile
     */
    public void init(String configFile) throws Exception {
        try{
            Properties props = new Properties();
            props.load(new FileInputStream(configFile));
            consumer = new KafkaConsumer<String, String>(props);
        }catch (Exception e){
            throw new Exception("init consumer error"+ e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            this.init("src/main/resources/kafkaproducer.properties");
            consumer.subscribe(Arrays.asList("topic1"), new ConsumerRebalanceListener() {
                        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                            LOG.info("threadId = {}, onPartitionsRevoked. "+ Thread.currentThread().getId());
                            consumer.commitSync(offsetsMap);
                        }

                        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                            LOG.info("threadId = {}, onPartitionsAssigned."+ Thread.currentThread().getId());
                            offsetsMap.clear();
                            //清空taskList列表
                            taskList.clear();
                        }
                    });

            //订阅kafka消息
            while(StautsCache.getInstance().isKafkaThreadStatus()){
                try{
                    //使用100ms作为获取超时时间
                    ConsumerRecords<String,String> records = consumer.poll(100);
                    for (ConsumerRecord record : records) {
                        semaphore.acquire();
                        //记录当前 TopicPartition和OffsetAndMetadata
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        OffsetAndMetadata offset = new OffsetAndMetadata(record.offset());
                        offsetsMap.put(topicPartition, offset);

                        //提交任务到线程池处理
                        taskList.add(workExecutorService.submit(new WorkThread(record.topic(), record.value().toString(), semaphore)));
                    }
                    //判断kafka消息是否处理完成
                    for(Future<String> task : taskList){
                        //阻塞，直到消息处理完成
                        task.get();
                    }

                    //同步向kafka集群中提交offset
                    consumer.commitSync();
                } catch (Exception e) {
                    LOG.error("TopicPartitionThread run error.", e);
                } finally{
                    //清空taskList列表
                    taskList.clear();
                }
                //关闭comsumer连接
                consumer.close();
            }
        } catch (Exception e) {
            LOG.error("init error.", e);
        }

    }
}
