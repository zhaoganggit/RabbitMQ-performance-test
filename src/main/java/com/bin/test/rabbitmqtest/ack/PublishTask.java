package com.bin.test.rabbitmqtest.ack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by joy on 9/1/16.
 */
public class PublishTask implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(PublishTask.class);

    private RabbitTemplate rabbitTemplate;
    private String         exchange;
    private String         queue;
    private TestConfig     testConfig;
    private int   ticks;
    private int tickNum = 0;

    public PublishTask(RabbitTemplate rabbitTemplate, String exchange,String  queue,TestConfig testConfig, int ticks){
        this.rabbitTemplate = rabbitTemplate;
        this.exchange = exchange;
        this.queue = queue;
        this.testConfig = testConfig;
        this.ticks = ticks;
    }

    @Override
    public void run() {
        while (++tickNum < ticks){
            pushMessages(ticks);
            holdOnFor(testConfig.getInterval());
        }
    }

    private void pushMessages(int tick){
        List<TestMessage> testMessages = buildMessages(testConfig.getBatchSize());
        for(int j=0;j<testMessages.size();j++){
            TestMessage testMessage = testMessages.get(j);
            rabbitTemplate.convertAndSend(exchange, queue,testMessage);
        }
        logger.info("push {} messages to queue {} in tick {}", testConfig.getBatchSize(),queue,tick);
    }

    private List<TestMessage> buildMessages(int batchSize){
        List<TestMessage> testMessages = new ArrayList<>();
        for(int i=0;i<batchSize;i++){
            String text = "abcdefghijkimn0123456789-" + i;
            TestMessage testMessage = new TestMessage(text);
            testMessages.add(testMessage);
        }
        return testMessages;
    }

    private static void holdOnFor(long millis){
        try {
            Thread.currentThread().sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
