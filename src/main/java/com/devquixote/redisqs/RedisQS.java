package com.devquixote.redisqs;

import static com.devquixote.redisqs.SqsReferenceOps.queueUrl;
import static com.devquixote.redisqs.SqsReferenceOps.setRegionEndpoint;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    public void setQueueAttributes(SetQueueAttributesRequest setQueueAttributesRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest)
                    throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest getQueueUrlRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void removePermission(RemovePermissionRequest removePermissionRequest)
            throws AmazonServiceException, AmazonClientException {}

    public GetQueueAttributesResult getQueueAttributes(GetQueueAttributesRequest getQueueAttributesRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
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
        String body = jedis.lpop(keyFor(request.getQueueUrl()));
        Message message = new Message();
        message.setBody(body);
        ReceiveMessageResult result = new ReceiveMessageResult();
        result.setMessages(Arrays.asList(message));
        return result;
    }

    public ListQueuesResult listQueues(ListQueuesRequest listQueuesRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
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
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public void deleteMessage(DeleteMessageRequest deleteMessageRequest)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public ListQueuesResult listQueues() throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void setQueueAttributes(String queueUrl, Map<String, String> attributes)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub

    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(String queueUrl,
            List<ChangeMessageVisibilityBatchRequestEntry> entries)
                    throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void changeMessageVisibility(String queueUrl, String receiptHandle, Integer visibilityTimeout)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub

    }

    public GetQueueUrlResult getQueueUrl(String queueName) throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void removePermission(String queueUrl, String label) throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub

    }

    public GetQueueAttributesResult getQueueAttributes(String queueUrl, List<String> attributeNames)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public SendMessageBatchResult sendMessageBatch(String queueUrl, List<SendMessageBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void deleteQueue(String queueUrl) throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub
        return null;
    }

    public DeleteMessageBatchResult deleteMessageBatch(String queueUrl, List<DeleteMessageBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public CreateQueueResult createQueue(String queueName) throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
        return null;
    }

    public void addPermission(String queueUrl, String label, List<String> aWSAccountIds, List<String> actions)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public void deleteMessage(String queueUrl, String receiptHandle)
            throws AmazonServiceException, AmazonClientException {
        // TODO Auto-generated method stub
    }

    public void shutdown() {
        // TODO Auto-generated method stub
    }

    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

    private String keyFor(String url) {
        return "sqs:" + url;
    }
}
