package com.bin.test.rabbitmqtest.ack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by joy on 9/1/16.
 */
public class TestMain {

    private static Logger logger = LoggerFactory.getLogger(TestMain.class);

    private static final String queuePrefix = "queue";

    private static List<String> queues = new ArrayList<>();

    public static void main(String[] args) {
        BeanFactory factory = new ClassPathXmlApplicationContext("applicationContext-test.xml");
        CachingConnectionFactory cachingConnectionFactory = (CachingConnectionFactory)factory.getBean("cachingConnectionFactory");
        RabbitAdmin rabbitAdmin = (RabbitAdmin)factory.getBean("rabbitAdmin");
        RabbitTemplate rabbitTemplate = (RabbitTemplate)factory.getBean("rabbitTemplate");
        TestConfig testConfig = (TestConfig)factory.getBean("testConfig");
        String exchangeName = "exchange-test";
        Exchange exchange = new DirectExchange(exchangeName, true, false);

        start(cachingConnectionFactory, testConfig, rabbitAdmin, rabbitTemplate, exchange, exchangeName);
        holdOnFor(3600 * 1000L);
    }

    private static void start(CachingConnectionFactory connectionFactory, TestConfig testConfig, RabbitAdmin rabbitAdmin,RabbitTemplate rabbitTemplate,Exchange exchange,String exchangeName){
        for(int i = 1; i <= testConfig.getQueuesCount(); i++){
            String queueName = queuePrefix + "-" + i;
            startWorkForQueue(connectionFactory, testConfig, rabbitAdmin,rabbitTemplate,exchange,exchangeName,queueName);
        }
    }

    private static void startWorkForQueue(CachingConnectionFactory connectionFactory, TestConfig testConfig, RabbitAdmin rabbitAdmin,RabbitTemplate rabbitTemplate,Exchange exchange,String exchangeName,String queueName){
        Random random = new Random();
        int ticks = random.nextInt(testConfig.getTicksBias());
        createQueue(rabbitAdmin, exchange, queueName);
        startConsume(connectionFactory, testConfig, rabbitAdmin, queueName, ticks);
        startPublish(testConfig, rabbitTemplate, exchangeName, queueName, ticks);
        logger.info("queue {} start with {} ticks", queueName, ticks);
    }

    private static void createQueue(RabbitAdmin rabbitAdmin, Exchange attemptExchange,String queueName){
        Queue queue = buildQueue(queueName);
        rabbitAdmin.declareQueue(queue);
        Binding binding = BindingBuilder.bind(queue).to(attemptExchange).with(queue.getName()).noargs();
        rabbitAdmin.declareBinding(binding);
        queues.add(queueName);
        logger.info("queue {} created",queueName);
    }

    private static void startConsume(CachingConnectionFactory connectionFactory, TestConfig testConfig, RabbitAdmin rabbitAdmin,String queueName,int ticks){
        ConsumerTask testConsumerTask = new ConsumerTask(connectionFactory,testConfig,queueName,rabbitAdmin,ticks);
        new Thread(testConsumerTask).start();
        logger.info("consumer launched for queue {}.",queueName);
    }

    private static void startPublish(TestConfig testConfig,RabbitTemplate rabbitTemplate,String exchangeName,String queueName,int ticks){
        PublishTask publishTask = new PublishTask(rabbitTemplate,exchangeName,queueName,testConfig,ticks);
        new Thread(publishTask).start();
        logger.info("publisher launched for queue {}.", queueName);
    }

    private static Queue buildQueue(String queueName) {
        Queue queue = new Queue(queueName, true, false, true);
        return queue;
    }

    private static void holdOnFor(long millis){
        try {
            Thread.currentThread().sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
