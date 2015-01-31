package it.polimi.kundera.client.azuretable;

import it.polimi.kundera.client.azuretable.config.AzureTableConstants;

/**
 * Aggregate partition key and row key into a single object.
 *
 * @author Fabio Arcidiacono.
 */
public class AzureTableKey {

    public static final int MAX_KEY_PARTS = 2;
    public static final String SEPARATOR = "_";
    private String rowKey;
    private String partitionKey;

    public AzureTableKey(String partitionKey, String rowKey) {
        this.partitionKey = partitionKey;
        this.rowKey = rowKey;
    }

    /**
     * Create a new instance of {@link AzureTableKey} from its string representation.
     *
     * @param rawKey the string representation of the key
     *
     * @throws java.lang.NullPointerException     if {@code rawKey} is null
     * @throws java.lang.IllegalArgumentException if {@code rawKey} is malformed
     */
    public AzureTableKey(String rawKey) {
        if (rawKey == null) {
            throw new NullPointerException("key cannot be null");
        }
        String[] parts = rawKey.split(SEPARATOR);
        if (parts.length == 0 || parts.length > MAX_KEY_PARTS) {
            throw new IllegalArgumentException("key [" + rawKey + "], is malformed and cannot be parsed");
        }
        if (parts.length == 1) {
            this.partitionKey = AzureTableConstants.getPartitionKey();
            this.rowKey = parts[0];
        } else {
            this.partitionKey = parts[0];
            this.rowKey = parts[1];
        }
    }

    public String getRowKey() {
        return rowKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    /**
     * Helper method to generate a String key given partition key and row key.
     *
     * @param partitionKey the partition key
     * @param rowKey       the row key
     *
     * @return a string representation of the whole key
     */
    public static String asString(String partitionKey, String rowKey) {
        return new AzureTableKey(partitionKey, rowKey).toString();
    }

    /**
     * Helper method to generate a String key given row key.
     * The partition key is set to the default one.
     * <p/>
     * Default partition key can be set through Azure Table specific properties
     * using the property name 'table.partition.default' in the xml file.
     *
     * @param rowKey the row key
     *
     * @return a string representation of the whole key
     */
    public static String asString(String rowKey) {
        return new AzureTableKey(AzureTableConstants.getPartitionKey(), rowKey).toString();
    }

    /**
     * Helper method to get the partition key from the given string representation
     * of a {@link AzureTableKey}.
     *
     * @param key the {@link AzureTableKey}
     *
     * @return the partition key
     */
    public static String getPrartitionKey(String key) {
        return new AzureTableKey(key).getPartitionKey();
    }

    /**
     * Helper method to get the row key from the given string representation
     * of a {@link AzureTableKey}.
     *
     * @param key the {@link AzureTableKey}
     *
     * @return the row key
     */
    public static String getRowKey(String key) {
        return new AzureTableKey(key).getRowKey();
    }

    @Override
    public String toString() {
        if (partitionKey.equals(AzureTableConstants.getPartitionKey())) {
            return rowKey;
        }
        return partitionKey + SEPARATOR + rowKey;
    }
}
