package it.polimi.kundera.client.azuretable.config;

/**
 * Constants defining properties that can be specified in datastore specific property file.
 *
 * @author Fabio Arcidiacono.
 */
public class AzureTableConstants {

    public static final String HTTP = "http";
    public static final String HTTPS = "https";
    public static final String LOCALHOST = "http://127.0.0.1";

    public static final String STORAGE_EMULATOR = "table.emulator";
    public static final String EMULATOR_PROXY = "table.emulator.proxy";

    public static final String PROTOCOL = "table.protocol";

    public static final String PARTITION_KEY = "table.partition.default";
    public static final String DEFAULT_PARTITION = "DEFAULT";
    private static String partitionKey;

    private AzureTableConstants() {
    }

    public static void setPartitionKey(String partitionKey) {
        AzureTableConstants.partitionKey = partitionKey;
    }

    public static String getPartitionKey() {
        return partitionKey;
    }
}
