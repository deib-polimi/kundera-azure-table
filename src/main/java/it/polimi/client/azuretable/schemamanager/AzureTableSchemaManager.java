package it.polimi.client.azuretable.schemamanager;

import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTableSchemaManager extends AbstractSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(AzureTableSchemaManager.class);

    /**
     * Initialise with configured client factory.
     *
     * @param clientFactory      specific client factory.
     * @param externalProperties external properties
     * @param kunderaMetadata    kundera metadata
     */
    public AzureTableSchemaManager(String clientFactory, Map<String, Object> externalProperties, EntityManagerFactoryImpl.KunderaMetadata kunderaMetadata) {
        super(clientFactory, externalProperties, kunderaMetadata);
    }

    @Override
    public void exportSchema(final String persistenceUnit, List<TableInfo> schemas) {
        //TODO
    }

    @Override
    protected boolean initiateClient() {
        //TODO
        return false;
    }

    /*
     * validates schema tables based on entity definition. Throws SchemaGenerationException if validation fails.
     */
    @Override
    protected void validate(List<TableInfo> tablesInfo) {
        //TODO
    }

    /*
     * updates schema tables based on entity definition.
     */
    @Override
    protected void update(List<TableInfo> tableInfo) {
        //TODO
    }

    /*
     * drops (if exists) schema and then creates schema tables based on entity definitions.
     */
    @Override
    protected void create(List<TableInfo> tableInfo) {
        //TODO
    }

    /*
     * drops (if exists) schema, creates schema tables based on entity definitions.
     */
    @Override
    protected void create_drop(List<TableInfo> tableInfo) {
        //TODO
    }

    /**
     * Method required to drop auto create schema, in case of schema operation as
     * {create-drop},
     */
    @Override
    public void dropSchema() {
        //TODO
    }

    @Override
    public boolean validateEntity(Class clazz) {
        return true;
    }
}
