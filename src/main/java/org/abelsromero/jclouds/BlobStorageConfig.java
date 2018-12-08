package org.abelsromero.jclouds;

public class BlobStorageConfig {

    private final String accountName;
    private final String accountKey;
    private final String containerName;

    public BlobStorageConfig(String accountName, String accountKey, String containerName) {
        this.accountName = accountName;
        this.accountKey = accountKey;
        this.containerName = containerName;
    }

    public String getAccountName() {
        return accountName;
    }

    public String getAccountKey() {
        return accountKey;
    }

    public String getContainerName() {
        return containerName;
    }

}
