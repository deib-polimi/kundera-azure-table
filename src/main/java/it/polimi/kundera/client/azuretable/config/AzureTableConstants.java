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

    private AzureTableConstants() {
    }
}
