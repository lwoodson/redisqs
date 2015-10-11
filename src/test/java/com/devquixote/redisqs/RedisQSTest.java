package com.devquixote.redisqs;

import java.util.ArrayList;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import redis.clients.jedis.Jedis;

public class RedisQSTest extends Assert {
    private RedisQS service;
    private Jedis jedis;
    private String queueName;
    private String queueUrl;
    private String queueKey;
    private String messageBody;

    @BeforeMethod
    public void setUp() {
        this.service = new RedisQS("localhost", 6379, 15);
        this.jedis = service.getJedis();
        this.queueName = "testQueue";
        this.queueUrl = "https://us-east-1/queue.amazonaws.com/123456789123/" + queueName;
        this.queueKey = "sqs:" + queueUrl;
        this.messageBody = "{'data': 'test'}";

        jedis.flushDB();
    }

    @Test
    public void ensureCreateQueueReturnsProperQueueResultWithQueueURL() {
        CreateQueueRequest request = new CreateQueueRequest(queueName);
        CreateQueueResult result = service.createQueue(request);
        assertEquals(result.getQueueUrl(), queueUrl);
    }

    @Test
    public void ensureSendMessagePushesOntoQueue() {
        SendMessageRequest request = new SendMessageRequest(queueUrl, messageBody);
        SendMessageResult result = service.sendMessage(request);
        assertNotNull(result);
        assertEquals(jedis.llen(queueKey), new Long(1));
    }

    @Test
    public void ensureReceiveMessagePopsOffOfQueue() {
        service.sendMessage(queueUrl, messageBody);
        service.sendMessage(queueUrl, messageBody);
        assertEquals(jedis.llen(queueKey), new Long(2));
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
        ReceiveMessageResult result = service.receiveMessage(request);
        assertEquals(result.getMessages().size(), 1);
        Message message = result.getMessages().get(0);
        assertEquals(message.getBody(), messageBody);
        assertEquals(jedis.llen(queueKey), new Long(1));
    }

    @Test
    public void ensureMessagesDequeuedInOrder() {
        service.sendMessage(queueUrl, "one");
        service.sendMessage(queueUrl, "two");
        service.sendMessage(queueUrl, "three");

        assertEquals(service.receiveMessage(queueUrl).getMessages().get(0).getBody(), "one");
        assertEquals(service.receiveMessage(queueUrl).getMessages().get(0).getBody(), "two");
        assertEquals(service.receiveMessage(queueUrl).getMessages().get(0).getBody(), "three");
    }
    
    @Test
    public void ensureListQueuesReturnsAllQueuesForEmptyPattern() {
        service.sendMessage(queueUrl, "one");
        ListQueuesResult result = service.listQueues("");
        assertEquals(result.getQueueUrls(), Arrays.asList(queueUrl));
    }
   
    @Test
    public void ensureListQueuesReturnsAllQueuesForSplatPattern() {
        service.sendMessage(queueUrl, "one");
        ListQueuesResult result = service.listQueues("*");
        assertEquals(result.getQueueUrls(), Arrays.asList(queueUrl));
    }

    @Test
    public void ensureListQueuesReturnsAllQueuesForNoPattern() {
        service.sendMessage(queueUrl, "one");
        ListQueuesResult result = service.listQueues();
        assertEquals(result.getQueueUrls(), Arrays.asList(queueUrl));
    }
    
    @Test
    public void ensureListQueuesReturnsQueuesMatchingPattern() {
        service.sendMessage(queueUrl, "one");
        ListQueuesResult result = service.listQueues("test");
        assertEquals(result.getQueueUrls(), Arrays.asList(queueUrl));
    }

    @Test
    public void ensureListQueuesDoesNotReturnQueuesNotMatchingPattern() {
        service.sendMessage(queueUrl, "one");
        ListQueuesResult result = service.listQueues("foos");
        assertEquals(result.getQueueUrls(), new ArrayList<String>());
    }
}
