# redisqs
[Amazon SQS java client](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/AmazonSQS.html)
backed by [Redis](http://redis.io/) for non-cloud deployments, offline
development, CI pipelines, etc...

## Basic Usage
```java
AmazonSQS sqs = new RedisQS("localhost", 6379, 1);
String queueUrl = sqs.createQueue("testQueue").getQueueUrl();
sqs.sendMessage(queueUrl, "test message");

ReceiveMessageResult result = sqs.receiveMessage(queueUrl);
assert result.getMessages().get(0).getBody().equals("test message");
```

The intent is for RedisQS to mimic the behavior of the Amazon Java SDK's
AmazonSQS implementation as closely as possible.  Refer to the SDK's javadocs
and the tests here for more information.

## Contributing
1. Fork it
2. Hack it
3. Push it
4. Send pull request
