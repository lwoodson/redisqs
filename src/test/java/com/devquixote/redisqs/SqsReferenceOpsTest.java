package com.devquixote.redisqs;

import static com.devquixote.redisqs.SqsReferenceOps.queueUrl;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SqsReferenceOpsTest extends Assert {
    @Test
    public void ensureQueueUrlReturnsRepresentativeEndpoint() {
        String result = queueUrl("testQueue");
        assertEquals(result, "https://us-east-1/queue.amazonaws.com/123456789123/testQueue");
    }
}
