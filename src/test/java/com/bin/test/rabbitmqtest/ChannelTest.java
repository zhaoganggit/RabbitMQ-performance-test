package com.bin.test.rabbitmqtest;

import com.everbridge.notification.expansion.model.Attempt;
import com.everbridge.notification.expansion.model.AttemptPath;
import com.everbridge.notification.expansion.model.support.AttemptPathType;
import com.rabbitmq.client.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jason on 16-8-26.
 */
public class ChannelTest {
    private static Logger logger = LoggerFactory.getLogger(ChannelTest.class);

    private static int queueCount = 50;
    private int perQueueDataCount = 25000;
    private String exchangeName = "exchangeForBroadcast";
    private static AtomicInteger[] duplicateArray = new AtomicInteger[queueCount + 1];

    static {
        for (int i = 0; i < queueCount + 1; i++) {
            duplicateArray[i] = new AtomicInteger();
        }
    }


    @Test public void generateQueues() {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
        cachingConnectionFactory.setHost("127.0.0.1");
        cachingConnectionFactory.setPort(5672);
        cachingConnectionFactory.setChannelCacheSize(25);
        cachingConnectionFactory.setCacheMode(CachingConnectionFactory.CacheMode.CHANNEL);

        Connection connection;
        Channel channel;
        try {
            connection = cachingConnectionFactory.createConnection();
            channel = connection.createChannel(false);
            channel.exchangeDeclare(exchangeName, "topic", true, false, null);
            for (int i = 0; i < queueCount; i++) {
                String queueName = "broadcast_" + (i + 1);
                String routingKey = "routing_" + (i + 1);
                channel.queueDeclare(queueName, true, false, false, null);
                channel.queueBind(queueName, exchangeName, routingKey);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        pushData();
    }

    @Test public void channelTest() throws InterruptedException {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
        cachingConnectionFactory.setHost("127.0.0.1");
        cachingConnectionFactory.setPort(5672);
        cachingConnectionFactory.setChannelCacheSize(5);
        cachingConnectionFactory.setCacheMode(CachingConnectionFactory.CacheMode.CHANNEL);

        ExecutorService executorService = Executors.newFixedThreadPool(queueCount);

        while (true) {
            for (int i = 0; i < queueCount; i++) {
                ThreadTest threadTest = new ThreadTest(cachingConnectionFactory, "broadcast_" + (i + 1));
                executorService.execute(threadTest);
                if ((i + 1) % 5 == 0) {
                    logger.info("sleep 2s, wait for release channel");
                    Thread.sleep(2000);
                }
            }
        }

        //        Thread.sleep(5000);
        //        boolean flag = false;
        //        while (!flag) {
        //            for (int i = 1; i < duplicateArray.length; i++) {
        //                if (duplicateArray[i].get() == 0) {
        //                    break;
        //                }
        //                flag = true;
        //            }
        //            Thread.sleep(500);
        //        }
    }

    private class ThreadTest implements Runnable {
        private CachingConnectionFactory cachingConnectionFactory;
        private String queueName;

        ThreadTest(CachingConnectionFactory cachingConnectionFactory, String queueName) {
            this.cachingConnectionFactory = cachingConnectionFactory;
            this.queueName = queueName;
        }

        @Override public void run() {
            Random random = new Random();
            int stopTime = random.nextInt(20) + 10;
            Channel channel = cachingConnectionFactory.createConnection().createChannel(false);
            //            logger.info("get channel number is {}", channel.getChannelNumber());
            try {
                channel.basicQos(1000);
                AttemptConsumer attemptConsumer = new AttemptConsumer(channel);
                int channelCount = duplicateArray[channel.getChannelNumber()].incrementAndGet();
                if (channelCount > 1) {
                    logger.error("has duplicateChannel!! queueName is {}", queueName);
                }
                long begin = System.currentTimeMillis();
                String consumerTag = channel.basicConsume(queueName, false, attemptConsumer);
                while (!attemptConsumer.shutdown) {

                }
                //                channel.basicCancel(consumerTag);
                //                if (channel.isOpen()) {
                //                    channel.close();
                //                }
                //                int channelCount2 = duplicateArray[channel.getChannelNumber()].decrementAndGet();
                //                if (channelCount2 >= 1) {
                //                    logger.error("has duplicateChannel!! queueName is {}", queueName);
                //                }
                long end = System.currentTimeMillis();
                //                logger.info("queue " + queueName + " ,Time is " + new Date(begin) + " to " + new Date(end));
            } catch (Exception e) {
                logger.error("basic {} consumer, channel name is {}, caused by: {}" + ExceptionUtils.getFullStackTrace(e), queueName,
                                channel.getChannelNumber());
            }
        }
    }


    private class AttemptConsumer extends DefaultConsumer {
        int received = 0;
        boolean shutdown = false;

        AttemptConsumer(Channel channel) {
            super(channel);
        }

        @Override public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            logger.error("consumer {} has exception {}", consumerTag, ExceptionUtils.getFullStackTrace(sig));
        }


        @Override public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            received++;
            try {
                if (received >= 5000 && !shutdown) {
                    closeChannel(consumerTag);
                }
                if (!shutdown) {
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                }
            } catch (Exception e) {

            }
        }

        private void closeChannel(String consumerTag) {
            try {

                getChannel().basicCancel(consumerTag);
                if (getChannel().isOpen()) {
                    getChannel().close();
                }
                int channelCount = duplicateArray[getChannel().getChannelNumber()].decrementAndGet();
                if (channelCount >= 1) {
                    logger.error("has duplicateChannel!! consumerTag is {}", consumerTag);
                }
                shutdown = true;
            } catch (Exception e) {
                logger.error("error, caused by: {}", ExceptionUtils.getFullStackTrace(e));
            }

        }
    }


    private void pushData() {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
        cachingConnectionFactory.setHost("127.0.0.1");
        cachingConnectionFactory.setPort(5672);
        cachingConnectionFactory.setChannelCacheSize(100);
        cachingConnectionFactory.setCacheMode(CachingConnectionFactory.CacheMode.CHANNEL);

        RabbitTemplate rabbitTemplate = new RabbitTemplate();
        rabbitTemplate.setConnectionFactory(cachingConnectionFactory);
        MessageConverter convert = new JsonMessageConverter();

        Attempt attempt = new Attempt();
        attempt.setAttempt(1);
        attempt.setBroadcastId("56fbd14ae4b0533bca1fd624");
        attempt.setContactId("444249942261817");
        attempt.setFirstName("echo");
        attempt.setLastName("xu");

        AttemptPath attemptPath = new AttemptPath();
        attemptPath.setPathType(AttemptPathType.DUMMY);
        attemptPath.setPathValue("abcdkededdada");
        attemptPath.setCallerId("12121212");
        attemptPath.setCountryCode("US");
        attemptPath.setConfirmationPhoneNumber("112112121");

        attempt.setAttemptPath(attemptPath);
        for (int i = 0; i < queueCount; i++) {
            for (int j = 0; j < perQueueDataCount; j++) {
                rabbitTemplate.convertAndSend(exchangeName, "routing_" + (i + 1), attempt);
            }
        }
    }


}
