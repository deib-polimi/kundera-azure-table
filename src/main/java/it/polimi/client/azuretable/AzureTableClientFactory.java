package it.polimi.client.azuretable;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityReader;
import it.polimi.client.azuretable.config.AzureTablePropertyReader;
import it.polimi.client.azuretable.schemamanager.AzureTableSchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Used by Kundera to instantiate the Client.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.loader.GenericClientFactory
 */
public class AzureTableClientFactory extends GenericClientFactory {

    private static Logger logger = LoggerFactory.getLogger(AzureTableClientFactory.class);
    private EntityReader reader;
    private SchemaManager schemaManager;

    @Override
    public void initialize(Map<String, Object> puProperties) {
        //TODO
        reader = new AzureTableEntityReader(kunderaMetadata);
        initializePropertyReader();
        setExternalProperties(puProperties);
    }

    @Override
    protected Object createPoolOrConnection() {
        // TODO
        return null;
    }

    @Override
    protected Client instantiateClient(String persistenceUnit) {
        // TODO
        return null;
    }

    @Override
    public boolean isThreadSafe() {
        // TODO
        return false;
    }

    @Override
    public void destroy() {
        // TODO
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties) {
        if (schemaManager == null) {
            initializePropertyReader();
            setExternalProperties(puProperties);
            schemaManager = new AzureTableSchemaManager(this.getClass().getName(), puProperties, kunderaMetadata);
        }
        return schemaManager;
    }

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName) {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }

    private void initializePropertyReader() {
        if (propertyReader == null) {
            propertyReader = new AzureTablePropertyReader(externalProperties, kunderaMetadata.getApplicationMetadata()
                    .getPersistenceUnitMetadata(getPersistenceUnit()));
            propertyReader.read(getPersistenceUnit());
        }
    }
}
