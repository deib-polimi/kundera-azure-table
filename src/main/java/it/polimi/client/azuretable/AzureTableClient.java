package it.polimi.client.azuretable;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import it.polimi.client.azuretable.query.AzureTableQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * The gateway to CRUD operations on database, except for queries.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.client.ClientBase
 * @see com.impetus.kundera.client.Client
 * @see it.polimi.client.azuretable.query.AzureTableQuery
 * @see com.impetus.kundera.generator.AutoGenerator
 */
public class AzureTableClient extends ClientBase implements Client<AzureTableQuery>, AutoGenerator {

    private EntityReader reader;
    private CloudTableClient tableClient;
    private static final Logger logger;

    static {
        logger = LoggerFactory.getLogger(AzureTableClient.class);
    }

    protected AzureTableClient(final KunderaMetadata kunderaMetadata, Map<String, Object> properties,
                               String persistenceUnit, final ClientMetadata clientMetadata, IndexManager indexManager,
                               EntityReader reader, CloudTableClient tableClient) {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.tableClient = tableClient;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
    }

    @Override
    public void close() {
        this.indexManager.flush();
        this.reader = null;
        this.tableClient = null;
        externalProperties = null;
    }

    @Override
    public EntityReader getReader() {
        return reader;
    }

    @Override
    public Class<AzureTableQuery> getQueryImplementor() {
        return AzureTableQuery.class;
    }

    @Override
    public Object generate() {
        // TODO partition key maybe the same for every row in the same table ?
        String partitionKey = UUID.randomUUID().toString();
        String rowKey = UUID.randomUUID().toString();
        return new AzureTableKey(partitionKey, rowKey).toString();
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- PERSIST OPERATIONS --------------------------------*/
    /*---------------------------------------------------------------------------------*/

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {
        logger.debug("entityMetadata = [" + entityMetadata + "], entity = [" + entity + "], id = [" + id + "], rlHolders = [" + rlHolders + "]");

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata,
                entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        AzureTableKey key = new AzureTableKey(id.toString());
        DynamicEntity tableEntity = new DynamicEntity(key.getPartitionKey(), key.getRowKey());

        handleAttributes(tableEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(tableEntity, entityMetadata, rlHolders);
        /* discriminator column is used for JPA inheritance */
        handleDiscriminatorColumn(tableEntity, entityType);

        try {
            TableOperation insertOperation = TableOperation.insertOrReplace(tableEntity);
            tableClient.execute(entityMetadata.getTableName(), insertOperation);
            logger.info(tableEntity.toString());
        } catch (StorageException e) {
            throw new KunderaException("Some error occurred while persisting entity: ", e);
        }
    }

    private void handleAttributes(DynamicEntity tableEntity, Object entity, MetamodelImpl metamodel, EntityMetadata entityMetadata, Set<Attribute> attributes) {
        String idAttribute = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        for (Attribute attribute : attributes) {
            // By pass ID attribute, is redundant since is also stored within the Key.
            // By pass associations (i.e. relations) that are handled in handleRelations()
            if (!attribute.isAssociation() && !((AbstractAttribute) attribute).getJPAColumnName().equals(idAttribute)) {
                if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                    processEmbeddableAttribute(tableEntity, entity, attribute, metamodel);
                } else {
                    processAttribute(tableEntity, entity, attribute);
                }
            }
        }
    }

    private void processAttribute(DynamicEntity tableEntity, Object entity, Attribute attribute) {
        Field field = (Field) attribute.getJavaMember();
        Object valueObj = PropertyAccessorHelper.getObject(entity, field);
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();

        if (valueObj instanceof Collection<?> || valueObj instanceof Map<?, ?>) {
            try {
                logger.debug("field = [" + field.getName() + "], objectType = [" + valueObj.getClass().getName() + "]");
                valueObj = AzureTableUtils.serialize(valueObj);
            } catch (IOException e) {
                throw new KunderaException("Some errors occurred while serializing the object: ", e);
            }
        } else if (((Field) attribute.getJavaMember()).getType().isEnum()) {
            valueObj = valueObj.toString();
        }
        if (valueObj != null) {
            logger.debug("field = [" + field.getName() + "], jpaColumnName = [" + jpaColumnName + "], valueObj = [" + valueObj + "]");
            AzureTableUtils.setPropertyHelper(tableEntity, jpaColumnName, valueObj);
        }
    }

    private void processEmbeddableAttribute(DynamicEntity tableEntity, Object entity, Attribute attribute, MetamodelImpl metamodel) {
        Field field = (Field) attribute.getJavaMember();
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        Object embeddedObj = PropertyAccessorHelper.getObject(entity, field);
        logger.debug("field = [" + field.getName() + "], jpaColumnName = [" + jpaColumnName + "], embeddedObj = [" + embeddedObj + "]");

        //embedded attributes are not supported by AzureTable, they must be serialized
        try {
            embeddedObj = AzureTableUtils.serialize(embeddedObj);
            AzureTableUtils.setPropertyHelper(tableEntity, jpaColumnName, embeddedObj);
        } catch (IOException e) {
            throw new KunderaException("Some errors occurred while serializing the object: ", e);
        }
    }

    private void handleRelations(DynamicEntity tableEntity, EntityMetadata entityMetadata, List<RelationHolder> rlHolders) {
        /*
         * TODO
         * primary key for and entity is the pair (partitionKey, rowKey)
         *
         * TableOperation retrieveOperation = TableOperation.retrieve(partitionKey, rowKey, DynamicEntity.class);
         * CustomerEntity specificEntity = cloudTable.execute(retrieveOperation).getResultAsType();
         */
    }

    private void handleDiscriminatorColumn(DynamicEntity tableEntity, EntityType entityType) {
        String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discriminatorValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (discriminatorColumn != null && discriminatorValue != null) {
            logger.debug("discriminatorColumn = [" + discriminatorColumn + "], discriminatorValue = [" + discriminatorValue + "]");
            AzureTableUtils.setPropertyHelper(tableEntity, discriminatorColumn, discriminatorValue);
        }
    }

    /*
     * persist join table for ManyToMany
     *
     * for example:
     *  -----------------------------------------------------------------------
     *  |                    EMPLOYEE_PROJECT (joinTableName)                  |
     *  -----------------------------------------------------------------------
     *  | EMPLOYEE_ID (joinColumnName)  |  PROJECT_ID (inverseJoinColumnName)  |
     *  -----------------------------------------------------------------------
     *  |          id (owner)           |             id (child)               |
     *  -----------------------------------------------------------------------
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        //TODO
    }

    /*---------------------------------------------------------------------------------*/
    /*------------------------------ FIND OPERATIONS ----------------------------------*/
    /*---------------------------------------------------------------------------------*/

    /*
     * it's called to find detached entities
     */
    @Override
    public Object find(Class entityClass, Object id) {
        logger.debug("entityClass = [" + entityClass.getSimpleName() + "], id = [" + id + "]");

        AzureTableKey key = new AzureTableKey(id.toString());
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        return get(entityMetadata.getTableName(), key);
    }

    private DynamicEntity get(String table, AzureTableKey key) {
        TableOperation retrieveOperation = TableOperation.retrieve(key.getPartitionKey(), key.getRowKey(), DynamicEntity.class);
        try {
            DynamicEntity tableEntity = tableClient.execute(table, retrieveOperation).getResultAsType();
            if (tableEntity == null) {
                logger.info("Not found {table = [" + table + "], key = [" + key.toString() + "]}");
            }
            return tableEntity;
        } catch (StorageException e) {
            throw new KunderaException("A problem occurred while retrieving the entity with key: " + key.toString(), e);
        }
    }

    /*
     * Implicitly it gets invoked, when kundera.indexer.class or lucene.home.dir is configured.
     * Means to use custom indexer for secondary indexes.
     * This method can also be very helpful to find rows for all primary keys! as with
     * em.getDelegate() you can get a handle of client object and can simply invoke findAll().
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        logger.debug("entityClass = [" + entityClass + "], columnsToSelect = [" + Arrays.toString(columnsToSelect) + "], keys = [" + Arrays.toString(keys) + "]");

        List results = new ArrayList();
        for (Object key : keys) {
            Object object = this.find(entityClass, key);
            if (object != null) {
                results.add(object);
            }
        }
        return results;
    }

    /*
     * It can be ignored, It was in place to purely support Cassandra's super columns.
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        throw new UnsupportedOperationException("Not supported in " + this.getClass().getSimpleName());
    }

    /*
     * used to retrieve relation for OneToMany (ManyToOne inverse relation),
     * is supposed to retrieve the initialized objects.
     *
     * for example:
     *      select * from EmployeeMTObis (entityClass)
     *      where DEPARTMENT_ID (colName) equals (colValue)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClass) {
        //TODO
        return null;
    }

    /*
     * used to retrieve owner-side relation for ManyToMany,
     * is supposed to retrieve the objects id.
     *
     * for example:
     *      select PROJECT_ID (columnName) from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (pKeyColumnName) equals (pKeyColumnValue)
     *
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        //TODO
        return null;
    }

    /*
     * used to retrieve target-side relation for ManyToMany,
     * is supposed to retrieve the objects id.
     *
     * for example:
     *      select EMPLOYEE_ID (pKeyName) from EMPLOYEE_PROJECT (tableName)
     *      where PROJECT_ID (columnName) equals (columnValue)
     *
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        //TODO
        return null;
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- DELETE OPERATIONS ---------------------------------*/
    /*---------------------------------------------------------------------------------*/

    @Override
    public void delete(Object entity, Object pKey) {
        logger.debug("entity = [" + entity + "], pKey = [" + pKey + "]");

        AzureTableKey key = new AzureTableKey(pKey.toString());
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        try {
            DynamicEntity tableEntity = get(entityMetadata.getTableName(), key);
            TableOperation deleteOperation = TableOperation.delete(tableEntity);
            tableClient.execute(entityMetadata.getTableName(), deleteOperation);
        } catch (StorageException e) {
            throw new KunderaException("A problem occurred while deleting the entity: ", e);
        }
    }

    /*
     * used to delete relation for ManyToMany
     *
     * for example:
     *      delete from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (columnName) equals (columnValue)
     *
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        //TODO
    }
}
