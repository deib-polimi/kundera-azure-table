package it.polimi.kundera.client.azuretable.config;

import com.impetus.kundera.configure.AbstractPropertyReader;
import com.impetus.kundera.configure.ClientProperties;
import com.impetus.kundera.configure.ClientProperties.DataStore;
import com.impetus.kundera.configure.PropertyReader;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

import java.util.Map;

/**
 * Reads datastore specific property from external file specified
 * through "kundera.client.property" property in persistence.xml.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.configure.AbstractPropertyReader
 * @see com.impetus.kundera.configure.PropertyReader
 */
public class AzureTablePropertyReader extends AbstractPropertyReader implements PropertyReader {

    public static AzureTableSchemaMetadata asm;

    public AzureTablePropertyReader(Map externalProperties, PersistenceUnitMetadata puMetadata) {
        super(externalProperties, puMetadata);
        asm = new AzureTableSchemaMetadata();
    }

    public void onXml(ClientProperties cp) {
        if (cp != null) {
            asm.setClientProperties(cp);
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
                    if (dataStore.getName() != null && "azure-table".equalsIgnoreCase(dataStore.getName().trim())) {
                        return dataStore;
                    }
                }
            }
            return null;
        }
    }
}
