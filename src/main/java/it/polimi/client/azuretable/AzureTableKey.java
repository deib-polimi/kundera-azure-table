package it.polimi.client.azuretable;

/**
 * Aggregate partition key and row key into a single object.
 *
 * @author Fabio Arcidiacono.
 */
public class AzureTableKey {

    private String rowKey;
    private String partitionKey;

    public AzureTableKey() {}

    public AzureTableKey(String partitionKey, String rowKey) {
        this.partitionKey = partitionKey;
        this.rowKey = rowKey;
    }

    public AzureTableKey(String rawKey) {
        if (rawKey == null) {
            throw new NullPointerException("key cannot be null");
        }
        String[] parts = rawKey.split("_");
        if (parts.length != 2) {
            throw new RuntimeException("key " + rawKey + ", is malformed and cannot be parsed");
        }
        this.partitionKey = parts[0];
        this.rowKey = parts[1];
    }

    public String getRowKey() {
        return rowKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public String toString() {
        return partitionKey + "_" + rowKey;
    }
}
