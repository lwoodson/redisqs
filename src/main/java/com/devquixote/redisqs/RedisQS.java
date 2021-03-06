package com.devquixote.redisqs;

import static com.devquixote.redisqs.SqsReferenceOps.queueUrl;
import static com.devquixote.redisqs.SqsReferenceOps.setRegionEndpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

import redis.clients.jedis.Jedis;

/**
 * AmazonSQS implementation backed by Redis.
 *
 * @author lance.woodson
 *
 */
public class RedisQS implements AmazonSQS {
    private Jedis jedis;

    /**
     * Create a new RedisQS using a Jedis client.
     *
     * @param jedis the ready-to-use Jedis client
     */
    public RedisQS(Jedis jedis) {
        this.jedis = jedis;
    }

    /**
     * Create a new RedisQS instance using explicit connection
     * details
     *
     * @param host the host machine where the redis server is found
     * @param port the port on which redis is listening
     * @param db the database number
     */
    public RedisQS(String host, Integer port, Integer db) {
        jedis = new Jedis(host, port);
        jedis.select(db);
    }

    /**
     * @return the Jedis instance communicating to the db.
     */
    public Jedis getJedis() {
        return jedis;
    }


    /**
     * Does nothing
     */
    public void setEndpoint(String endpoint) throws IllegalArgumentException {}
    /**
     * Does nothing
     */
    public void setRegion(Region region) throws IllegalArgumentException {
        setRegionEndpoint(region.getName());
    }

    public void setQueueAttributes(SetQueueAttributesRequest request)
            throws AmazonServiceException, AmazonClientException {
        jedis.hmset(attributesKeyFor(request.getQueueUrl()), request.getAttributes());
    }

    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest)
                    throws AmazonServiceException, AmazonClientException { return null; }

    public void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest)
            throws AmazonServiceException, AmazonClientException { }

    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest request)
            throws AmazonServiceException, AmazonClientException {
        String queueUrl = queueUrl(request.getQueueName());
        GetQueueUrlResult result = new GetQueueUrlResult();
        result.setQueueUrl(queueUrl);
        return result;
    }

    public void removePermission(RemovePermissionRequest removePermissionRequest)
            throws AmazonServiceException, AmazonClientException {}

    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest request)
            throws AmazonServiceException, AmazonClientException {
        Map<String, String> rawAttributes = jedis.hgetAll(attributesKeyFor(request.getQueueUrl()));
        Map<String, String> attributes = new HashMap<String, String>();
        GetQueueAttributesResult result = new GetQueueAttributesResult();

        for (String attribute : request.getAttributeNames()) {
            attributes.put(attribute, rawAttributes.get(attribute));
        }

        Long queueSize = jedis.llen(keyFor(request.getQueueUrl()));
        attributes.put("ApproximateNumberOfMessages", queueSize.toString());

        result.setAttributes(attributes);
        return result;
    }

    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest request)
            throws AmazonServiceException, AmazonClientException {
        SendMessageBatchResult result = new SendMessageBatchResult();
        result.setSuccessful(new ArrayList<SendMessageBatchResultEntry>());

        for (SendMessageBatchRequestEntry entry : request.getEntries()) {
            sendMessage(request.getQueueUrl(), entry.getMessageBody());
            SendMessageBatchResultEntry successfulEntry = new SendMessageBatchResultEntry();
            result.getSuccessful().add(successfulEntry);
        }

        return result;
    }

    public void purgeQueue(PurgeQueueRequest purgeQueueRequest) throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public ListDeadLetterSourceQueuesResult listDeadLetterSourceQueues(
            ListDeadLetterSourceQueuesRequest listDeadLetterSourceQueuesRequest)
                    throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void deleteQueue(DeleteQueueRequest deleteQueueRequest)
            throws AmazonServiceException, AmazonClientException {
        jedis.del(keyFor(deleteQueueRequest.getQueueUrl()));
    }

    public SendMessageResult sendMessage(SendMessageRequest request)
            throws AmazonServiceException, AmazonClientException {
        jedis.rpush(keyFor(request.getQueueUrl()), request.getMessageBody());
        SendMessageResult result = new SendMessageResult();
        // TODO properly populate result
        return result;
    }

    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest request)
            throws AmazonServiceException, AmazonClientException {
        ReceiveMessageResult result = new ReceiveMessageResult();
        List<Message> messages = new ArrayList<Message>();

        if (request.getMaxNumberOfMessages() == null || request.getMaxNumberOfMessages() < 1)
                request.setMaxNumberOfMessages(1);

        for (int i = 0; i < request.getMaxNumberOfMessages(); i++) {
            String body = jedis.lpop(keyFor(request.getQueueUrl()));
            if (body == null)
                break;
            Message message = new Message();
            message.setBody(body);
            messages.add(message);
        }

        result.setMessages(messages);
        return result;
    }

    public ListQueuesResult listQueues(ListQueuesRequest listQueuesRequest)
            throws AmazonServiceException, AmazonClientException {
        String queueNamePrefix = listQueuesRequest.getQueueNamePrefix();

        if (queueNamePrefix == null || queueNamePrefix.equals("")) {
            queueNamePrefix = "*";
        } else if (!queueNamePrefix.endsWith("*")) {
            queueNamePrefix = queueNamePrefix + "*";
        }

        Set<String> redisNames = jedis.keys(keyFor(queueUrl(queueNamePrefix)));
        
        ListQueuesResult result = new ListQueuesResult();
        List<String> queueUrls = new ArrayList<String>();
        for (String redisName : redisNames) {
            queueUrls.add(redisName.split(":", 2)[1]);
        }
        result.setQueueUrls(queueUrls);
        return result;
    }

    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Create a representative URL for the queue name in the request.  Do not
     * need to create a list as redis does that automatically when we push onto
     * it.
     */
    public CreateQueueResult createQueue(CreateQueueRequest createQueueRequest)
            throws AmazonServiceException, AmazonClientException {
        String queueUrl = queueUrl(createQueueRequest.getQueueName());
        CreateQueueResult result = new CreateQueueResult();
        result.setQueueUrl(queueUrl);
        return result;
    }

    public void addPermission(AddPermissionRequest addPermissionRequest)
            throws AmazonServiceException, AmazonClientException { }

    public void deleteMessage(DeleteMessageRequest deleteMessageRequest)
            throws AmazonServiceException, AmazonClientException { }

    public ListQueuesResult listQueues() throws AmazonServiceException, AmazonClientException {
        return listQueues("*");
    }

    public void setQueueAttributes(String queueUrl, Map<String, String> attributes)
            throws AmazonServiceException, AmazonClientException {
        setQueueAttributes(new SetQueueAttributesRequest(queueUrl, attributes));
    }

    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(String queueUrl,
            List<ChangeMessageVisibilityBatchRequestEntry> entries)
                    throws AmazonServiceException, AmazonClientException {
        return null;
    }

    public void changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout)
            throws AmazonServiceException, AmazonClientException { }

    public GetQueueUrlResult getQueueUrl(String queueName) throws AmazonServiceException, AmazonClientException {
        GetQueueUrlRequest request = new GetQueueUrlRequest(queueName);
        return getQueueUrl(request);
    }

    public void removePermission(String queueUrl, String label) throws AmazonServiceException, AmazonClientException { }

    public GetQueueAttributesResult getQueueAttributes(String queueUrl, List<String> attributeNames)
            throws AmazonServiceException, AmazonClientException {
        GetQueueAttributesRequest request = new GetQueueAttributesRequest(queueUrl, attributeNames);
        return getQueueAttributes(request);
    }

    public SendMessageBatchResult sendMessageBatch(String queueUrl, List<SendMessageBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        SendMessageBatchRequest request = new SendMessageBatchRequest(queueUrl, entries);
        return sendMessageBatch(request);
    }

    public void deleteQueue(String queueUrl) throws AmazonServiceException, AmazonClientException {
        deleteQueue(new DeleteQueueRequest(queueUrl));
    }

    public SendMessageResult sendMessage(String queueUrl, String messageBody)
            throws AmazonServiceException, AmazonClientException {
        SendMessageRequest request = new SendMessageRequest(queueUrl, messageBody);
        return sendMessage(request);
    }

    public ReceiveMessageResult receiveMessage(String queueUrl) throws AmazonServiceException, AmazonClientException {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl);
        return receiveMessage(request);
    }

    public ListQueuesResult listQueues(String queueNamePrefix) throws AmazonServiceException, AmazonClientException {
        ListQueuesRequest request = new ListQueuesRequest(queueNamePrefix);
        return listQueues(request);
    }

    public DeleteMessageBatchResult deleteMessageBatch(String queueUrl, List<DeleteMessageBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public CreateQueueResult createQueue(String queueName) throws AmazonServiceException, AmazonClientException {
        CreateQueueRequest request = new CreateQueueRequest(queueName);
        return createQueue(request);
    }

    public void addPermission(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions)
            throws AmazonServiceException, AmazonClientException { }

    public void deleteMessage(String queueUrl, String receiptHandle)
            throws AmazonServiceException, AmazonClientException { }

    public void shutdown() { }

    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) { return null; }

    private String keyFor(String url) {
        return "sqs:" + url;
    }

    private String attributesKeyFor(String url) {
        return keyFor(url) + ":attributes";
    }
}
