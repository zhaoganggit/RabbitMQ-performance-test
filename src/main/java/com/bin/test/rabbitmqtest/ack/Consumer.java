package com.bin.test.rabbitmqtest.ack;

import com.rabbitmq.client.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Consumer extends DefaultConsumer {

    private static Logger logger = LoggerFactory.getLogger(Consumer.class);
    private ObjectMapper jsonObjectMapper = new ObjectMapper();

    private String queueName;
    private String consumerTag;
    private int prefetchCount = 1000;
    private ConcurrentLinkedQueue<TestMessage> queue = new ConcurrentLinkedQueue();
    private AtomicLong deliveryTagCounter = null;
    private AtomicLong ackCounter = null;
    private boolean canceled = false;
    private Object cancelLocker = new Object();
    private boolean shutdown = false;


    public Consumer(String queueName, Channel channel) {
        super(channel);
        this.queueName = queueName;
    }

    @Override public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {

        try {
            super.handleDelivery(consumerTag, envelope, properties, body);
            if (null == deliveryTagCounter) {
                deliveryTagCounter = new AtomicLong(envelope.getDeliveryTag() - 1);
                ackCounter = new AtomicLong(envelope.getDeliveryTag() - 1);
                if(ackCounter.get() > 0) {
                    logger.info("the channel is reused. queueName: {}, start tag: {}", queueName, ackCounter.get());
                }
            }
            deliveryTagCounter.incrementAndGet();
            TestMessage testMessage = convert(body, properties.getContentEncoding());
            if (testMessage != null) {
                queue.add(testMessage);
            } else {
                logger.error("testMessage is null, queueName: {}, deliveryTag: {}", queueName, envelope.getDeliveryTag());
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("handleDelivery exception, queueName: {}, deliveryTag: {}", queueName, envelope.getDeliveryTag());
        }
    }

    /**
     * Called when a <code><b>basic.recover-ok</b></code> is received
     * in reply to a <code><b>basic.recover</b></code>. All messages
     * received before this is invoked that haven't been <i>ack</i>'ed will be
     * re-delivered. All messages received afterwards won't be.
     * @param consumerTag the <i>consumer tag</i> associated with the consumer
     */
    @Override public void handleRecoverOk(String consumerTag) {
        super.handleRecoverOk(consumerTag);
        logger.info("handleRecoverOk, queueName: {}", queueName);
    }

    /**
     * Called when the consumer is registered by a call to any of the
     * {@link Channel#basicConsume} methods.
     * @param consumerTag the <i>consumer tag</i> associated with the consumer
     */
    @Override public void handleConsumeOk(String consumerTag) {
        super.handleConsumeOk(consumerTag);
        logger.info("handleConsumeOk, queueName: {}", queueName);
    }

    /**
     * Called when the consumer is cancelled by a call to {@link Channel#basicCancel}.
     * @param consumerTag the <i>consumer tag</i> associated with the consumer
     */
    @Override public void handleCancelOk(String consumerTag) {
        super.handleCancelOk(consumerTag);
        logger.info("handleCancelOk, queueName: {}", queueName);
    }

    /**
     * Called when the consumer is cancelled for reasons <i>other than</i> by a call to
     * {@link Channel#basicCancel}. For example, the queue has been deleted.
     * @param consumerTag
     */
    @Override public void handleCancel(String consumerTag) throws IOException {
        super.handleCancel(consumerTag);
        logger.info("handleCancel, queueName: {}", queueName);
    }

    /**
     * Called when either the channel or the underlying connection has been shut down.
     * @param consumerTag the <i>consumer tag</i> associated with the consumer
     * @param sig a {@link ShutdownSignalException} indicating the reason for the shut down
     */
    @Override public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        super.handleShutdownSignal(consumerTag, sig);
        cancelConsume();
        logger.warn("handleShutdownSignal, queueName: {}, full stack trace: {}", queueName, ExceptionUtils.getFullStackTrace(sig));
        // the channel was shutdown, should recreate the consumer here.
        shutdown = true;
    }

    public List<TestMessage> readMessages(int numToRead) {
        List<TestMessage> testMessages = null;
        Boolean success = null;
        if(numToRead > 0) {
            int countRead = 0;
            testMessages = new ArrayList<>(numToRead);
            do {
                TestMessage testMessage = queue.poll();
                if (null != testMessage) {
                    testMessages.add(testMessage);
                    countRead++;
                } else {
                    break;
                }

            } while(countRead < numToRead);

            if(countRead > 0) {
                success = false;
                long ackDeliveryTag = ackCounter.addAndGet(countRead);
                success = sendAck(countRead, numToRead, ackDeliveryTag, 3);

            } else {
                logger.debug("no testMessages in cache for {} queue, desired {}", queueName, numToRead);
            }
        } else {
            testMessages = new ArrayList<>();
        }

        if(null != success && !success) {
            // ack failure, cancel consume first then flag consumer as shutdown.
            cancelConsume();
            shutdown = true;
            testMessages = new ArrayList<>();
        }

        return testMessages;
    }

    private boolean sendAck(int countRead, int numToRead, long ackDeliveryTag, int maxRetry) {
        int retries = 0;
        boolean success = false;
        do {
            try {
                // ack in batch
                getChannel().basicAck(ackDeliveryTag, true);
                success = true;
                logger.debug("read {} messages, desired: {}, ack in batch - tag: {}, deliveryTagCounter: {}, queueName: {}", countRead, numToRead, ackDeliveryTag, deliveryTagCounter, queueName);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("ack failed, ackDeliveryTag: {}, cause: {}", ackDeliveryTag, ExceptionUtils.getFullStackTrace(e));
            }
            retries++;
        } while (retries < maxRetry && !success);

        return success;
    }

    private TestMessage convert(byte[] body, String contentEncoding) {

        TestMessage testMessage = null;
        try {
            String contentAsString = new String(body, contentEncoding);
            testMessage = jsonObjectMapper.readValue(contentAsString, TestMessage.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("exception in converting raw message to testMessage. body: {}, contentEncoding: {}, cause: {}", body, contentEncoding, ExceptionUtils.getFullStackTrace(e));
        }

        return testMessage;
    }

    public void startConsume() {
        try {
            getChannel().basicQos(prefetchCount);
            consumerTag = getChannel().basicConsume(queueName, false, this);
            logger.info("start to consume queue: {}, consumerTag: {}, channel number: {}", queueName, consumerTag, getChannel().getChannelNumber());
        } catch (Exception e) {
            shutdown = true;
            e.printStackTrace();
            logger.error("basicConsume throws IOException for queue {}, cause: {}", queueName, ExceptionUtils.getFullStackTrace(e));
        }
    }

    public void cancelConsume() {
        synchronized (cancelLocker) {
            try {
                logger.info("start to cancel to consume queue: {}, consumerTag: {}", queueName, consumerTag);
                if(null != consumerTag && !canceled) {
                    getChannel().basicCancel(consumerTag);
                    canceled = true;
                }
                // return the channel to cache, see comments in CachingConnectionFactory.
                if (getChannel().isOpen()) {
                    getChannel().close();
                }
                logger.info("succeed to cancel to consume queue: {}, consumerTag: {}", queueName, consumerTag);
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("basicCancel throws Exception for queue {}, consumerTag: {}, cause: {}", queueName, consumerTag, ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public void setShutdown(boolean shutdown) {
        this.shutdown = shutdown;
    }
}
