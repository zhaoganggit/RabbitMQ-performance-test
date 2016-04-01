package com.bin.test.rabbitmqtest;

import com.everbridge.notification.expansion.model.Attempt;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import java.io.IOException;

/**
 * Created by jason on 16-3-31.
 */
public class BasicGetTest {
    private static Logger logger = LoggerFactory.getLogger(BasicGetTest.class);

    public static void main(String[] args) {
        Options options = getOptions();
        CommandLineParser parser = new GnuParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            String host = strArg(cmd, 'h', "127.0.0.1");
            String queueName = strArg(cmd, 'u', null);
            int consumerCount = intArg(cmd, 'y', 1);
            int channelCount = intArg(cmd, 'c', 1);
            int consumerPrefetchCount = intArg(cmd, 'q', 200);
            String username = strArg(cmd, 'n', null);
            String password = strArg(cmd, 'p', null);
            String vhost = strArg(cmd, 'v', null);
            consumer(host, consumerCount, channelCount, queueName, consumerPrefetchCount, username, password, vhost);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    private static Options getOptions() {
        Options options = new Options();
        options.addOption(new Option("h", "host", true, "connection host"));
        options.addOption(new Option("n", "username", true, "username"));
        options.addOption(new Option("p", "password", true, "password"));
        options.addOption(new Option("v", "vhost", true, "vhost"));
        options.addOption(new Option("y", "consumers", true, "consumer count"));
        options.addOption(new Option("c", "channels", true, "channel count"));
        options.addOption(new Option("u", "queue", true, "queue name"));
        options.addOption(new Option("q", "qos", true, "consumer prefetch count"));
        return options;
    }

    private static String strArg(CommandLine cmd, char opt, String def) {
        return cmd.getOptionValue(opt, def);
    }

    private static int intArg(CommandLine cmd, char opt, int def) {
        return Integer.parseInt(cmd.getOptionValue(opt, Integer.toString(def)));
    }


    private static void consumer(String host, int consumerCount, int channelCount, String queue, int consumerPrefetchCount, String username,
                    String password, String vhost) throws IOException {
        MessageConverter convert = new JsonMessageConverter();
        for (int i = 0; i < consumerCount; i++) {
            CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory();
            cachingConnectionFactory.setAddresses(host);
            cachingConnectionFactory.setCacheMode(CachingConnectionFactory.CacheMode.CHANNEL);
            cachingConnectionFactory.setChannelCacheSize(25);
            if (StringUtils.isNotBlank(username)) {
                cachingConnectionFactory.setUsername(username);
            }
            if (StringUtils.isNotBlank(password)) {
                cachingConnectionFactory.setPassword(password);
            }
            if (StringUtils.isNotBlank(vhost)) {
                cachingConnectionFactory.setVirtualHost(vhost);
            }
            for (int j = 0; j < channelCount; j++) {
                RabbitTemplate rabbitTemplate = new RabbitTemplate();
                rabbitTemplate.setConnectionFactory(cachingConnectionFactory);
                Thread consumerThread = new Thread(() -> {
                    while (true) {
                        int countRead = 0;
                        long startTime = System.currentTimeMillis();
                        while (countRead < consumerPrefetchCount) {
                            countRead++;
                            Message rawMessage = rabbitTemplate.receive(queue);
                            Attempt attempt = null;
                            if(rawMessage != null){
                                attempt = (Attempt) convert.fromMessage(rawMessage);
                            }
                        }
                        long endTime = System.currentTimeMillis();
                        logger.info("Thread Name {},take {} cost " + (endTime - startTime) + " ms", Thread.currentThread().getName(), countRead);
                    }

                });
                consumerThread.start();
            }

        }
    }
}
