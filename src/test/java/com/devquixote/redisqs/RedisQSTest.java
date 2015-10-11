package com.devquixote.redisqs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import redis.clients.jedis.Jedis;

public class RedisQSTest extends Assert {
    private RedisQS service;
    private Jedis jedis;
    private String queueName;
    private String queueUrl;
    private String queueKey;
    private String queueAttributesKey;
    private String messageBody;
    private Map<String, String> attributes;

    @BeforeMethod
    public void setUp() {
        this.service = new RedisQS("localhost", 6379, 15);
        this.jedis = service.getJedis();
        this.queueName = "testQueue";
        this.queueUrl = "https://us-east-1/queue.amazonaws.com/123456789123/" + queueName;
        this.queueKey = "sqs:" + queueUrl;
        this.queueAttributesKey = this.queueKey + ":attributes";
        this.messageBody = "{'data': 'test'}";
        attributes = new HashMap<String, String>();
        attributes.put("foo", "bar");

        jedis.flushDB();
    }

    @Test
    public void ensureCreateQueueReturnsProperQueueResultWithQueueURL() {
        CreateQueueResult result = service.createQueue(queueName);
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
    
    @Test
    public void ensureGetQueueUrlReturnsProperUrl() {
        assertEquals(service.getQueueUrl(queueName).getQueueUrl(), queueUrl);
    }
    
    @Test
    public void ensureDeleteQueueDeletesAQueue() {
        service.sendMessage(queueUrl, "one");
        assertTrue(jedis.exists(queueKey));
        service.deleteQueue(queueUrl);
        assertFalse(jedis.exists(queueKey));
    }

    @Test
    public void ensureSetQueueAttributesPersistsAttributesInRedisHash() {
        service.setQueueAttributes(queueUrl, attributes);
        assertEquals(jedis.hget(queueAttributesKey, "foo"), "bar");
    }

    @Test
    public void ensureGetAttributesContainsSpecifiedAttributes() {
        service.setQueueAttributes(queueUrl, attributes);
        GetQueueAttributesResult result = service.getQueueAttributes(queueUrl, Arrays.asList("foo"));
        assertEquals(result.getAttributes().get("foo"), "bar");
    }

    @Test
    public void ensureGetAttributesDoesNotContainNotSpecifiedAttributes() {
        service.setQueueAttributes(queueUrl, attributes);
        GetQueueAttributesResult result = service.getQueueAttributes(queueUrl, Arrays.asList("bar"));
        assertEquals(result.getAttributes().get("foo"), null);
    }

    @Test
    public void ensureGetAttributesContainsApproximateNumberOfMessages() {
        service.sendMessage(queueUrl, "one");
        service.sendMessage(queueUrl, "one");
        GetQueueAttributesResult result = service.getQueueAttributes(queueUrl, Arrays.asList("ApproximateNumberOfMessages"));
        assertEquals(Long.parseLong(result.getAttributes().get("ApproximateNumberOfMessages")), 2);
    }

    @Test
    public void sendMessageBatchEnqueuesAllMessagesInTheBatch() {
        List<SendMessageBatchRequestEntry> batchEntries = Arrays.asList(
                new SendMessageBatchRequestEntry("1", messageBody),
                new SendMessageBatchRequestEntry("2", messageBody),
                new SendMessageBatchRequestEntry("3", messageBody)
        );
        service.sendMessageBatch(queueUrl, batchEntries);
        assertEquals(jedis.llen(queueKey), new Long(3));
    }
}
