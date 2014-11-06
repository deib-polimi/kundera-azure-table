package it.polimi.client.azuretable.config;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTablePropertyReader extends AbstractPropertyReader implements PropertyReader {

    private static Logger logger = LoggerFactory.getLogger(AzureTablePropertyReader.class);
    public static AzureTableSchemaMetadata atsm;

    public AzureTablePropertyReader(Map externalProperties, PersistenceUnitMetadata puMetadata) {
        super(externalProperties, puMetadata);
        atsm = new AzureTableSchemaMetadata();
    }

    public void onXml(ClientProperties cp) {
        if (cp != null) {
            atsm.setClientProperties(cp);
        }
    }

    public class AzureTableSchemaMetadata {

        private ClientProperties clientProperties;

        private AzureTableSchemaMetadata() {
        }

        public ClientProperties getClientProperties() {
            return clientProperties;
        }

        private void setClientProperties(ClientProperties clientProperties) {
            this.clientProperties = clientProperties;
        }

        public DataStore getDataStore() {
            if (getClientProperties() != null && getClientProperties().getDatastores() != null) {
                for (DataStore dataStore : getClientProperties().getDatastores()) {
                    if (dataStore.getName() != null && dataStore.getName().trim().equalsIgnoreCase("azuretable")) {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
