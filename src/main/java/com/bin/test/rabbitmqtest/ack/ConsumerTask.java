package com.bin.test.rabbitmqtest.ack;

import com.rabbitmq.client.Channel;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import java.util.ArrayList;
import java.util.List;

public class ConsumerTask implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(ConsumerTask.class);

    private CachingConnectionFactory connectionFactory;
    private final String queueName;
    private RabbitAdmin  rabbitAdmin = null;
    private TestConfig consumeConfig;

    private int   ticks;
    private int   tickNum = 0;

    private Consumer testConsumer = null;

    public ConsumerTask(CachingConnectionFactory connectionFactory, TestConfig consumeConfig, String queueName, RabbitAdmin rabbitAdmin, int ticks) {
        this.connectionFactory = connectionFactory;
        this.consumeConfig = consumeConfig;
        this.queueName = queueName;
        this.rabbitAdmin = rabbitAdmin;
        this.ticks = ticks;
        try {
            Channel channel = getChannel();
            testConsumer = new Consumer(queueName, channel);
            testConsumer.startConsume();
        } catch (Exception e) {
            logger.error("channel connect error, caused by:{}", ExceptionUtils.getFullStackTrace(e));
        }
    }

    public List<TestMessage> getTestMessageBatch(int batchSize) {
        if(testConsumer == null || testConsumer.isShutdown()) {
            logger.info("recreate consumer for queue: {} due to previous one being shutdown.", queueName);
            try{
                Channel channel = getChannel();
                testConsumer = new Consumer(queueName, channel);
                testConsumer.startConsume();
            } catch (Exception e) {
                logger.error("channel connect for queue: {} error, caused by:{}", queueName, ExceptionUtils.getFullStackTrace(e));
            }
        }
        List<TestMessage> testMessages = new ArrayList<>();
        if (testConsumer != null) {
            testMessages = testConsumer.readMessages(batchSize);
        }
        return testMessages;
    }

    public void cleanupAndDeleteQueue() {
        logger.info("Deleting queue {}", queueName);
        try {
            cancelConsumption();
            boolean result = rabbitAdmin.deleteQueue(queueName);
            if(!result) {
                logger.error("Deleting queue {} return result {}", queueName, result);
            }
        } catch (Exception e) {
            logger.error("Error ({}) while deleting queue {}.", e.getMessage(), queueName);
        }
    }

    private Channel getChannel() throws Exception{
        return connectionFactory.createConnection().createChannel(false);
    }

    public void cancelConsumption() {
        if(testConsumer != null) {
            testConsumer.cancelConsume();
            logger.info("cancel to consume caused by consumption {} queue", queueName);
        }
    }

    @Override
    public void run() {
        while ( ++tickNum < ticks){
            getTestMessageBatch(consumeConfig.getBatchSize());
            try {
                Thread.currentThread().sleep(consumeConfig.getInterval());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        cleanupAndDeleteQueue();
    }
}
