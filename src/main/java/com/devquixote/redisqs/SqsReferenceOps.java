package com.devquixote.redisqs;

public abstract class SqsReferenceOps {
    public static String PROTOCOL = "https";
    public static String SERVICE_PATH = "queue.amazonaws.com";
    private static String regionEndpoint = "us-east-1";
    private static String accountNumber = "123456789123";

    public static String getRegionEndpoint() {
        return regionEndpoint;
    }

    public static void setRegionEndpoint(String regionEndpoint) {
        SqsReferenceOps.regionEndpoint = regionEndpoint;
    }

    public static String getAccountNumber() {
        return accountNumber;
    }

    public static void setAccountNumber(String accountNumber) {
        SqsReferenceOps.accountNumber = accountNumber;
    }

    public static String queueUrl(String queueName) {
        StringBuffer buff = new StringBuffer();
        buff.append(PROTOCOL).append("://")
            .append(getRegionEndpoint()).append("/")
            .append(SERVICE_PATH).append("/")
            .append(getAccountNumber()).append("/")
            .append(queueName);
        return buff.toString();
    }
}
