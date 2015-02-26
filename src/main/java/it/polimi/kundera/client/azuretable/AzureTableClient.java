package it.polimi.kundera.client.azuretable;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.property.accessor.EnumAccessor;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.*;
import it.polimi.kundera.client.azuretable.query.AzureTableQuery;
import it.polimi.kundera.client.azuretable.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.*;

/**
 * The gateway to CRUD operations on database, except for queries.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.client.ClientBase
 * @see com.impetus.kundera.client.Client
 * @see it.polimi.kundera.client.azuretable.query.AzureTableQuery
 * @see com.impetus.kundera.generator.AutoGenerator
 */
public class AzureTableClient extends ClientBase implements Client<AzureTableQuery>, AutoGenerator {

    private EntityReader reader;
    private CloudTableClient tableClient;
    private static final Logger logger = LoggerFactory.getLogger(AzureTableClient.class);

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
        // return a random UUID as row key, the partition key is implicitly the default one.
        return UUID.randomUUID().toString();
    }

    /*---------------------------------------------------------------------------------*/
    /*----------------------------- PERSIST OPERATIONS --------------------------------*/
    /*---------------------------------------------------------------------------------*/

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {
        logger.debug("entityMetadata = [" + entityMetadata + "], entity = [" + entity + "], id = [" + id + "], rlHolders = [" + rlHolders + "]");

        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata, entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        DynamicEntity tableEntity = AzureTableUtils.createDynamicEntity(entityMetadata, id);

        handleAttributes(tableEntity, entity, metamodel, entityMetadata, entityType.getAttributes());
        handleRelations(tableEntity, entityMetadata, rlHolders);
        // discriminator column is used for JPA inheritance
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
            // by pass ID attribute, is redundant since is also stored within the Key.
            // by pass associations (i.e. relations) that are handled in handleRelations()
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

        //embedded objects are not supported by AzureTable, they must be deeply serialized
        try {
            EmbeddedEntity embeddedEntity = new EmbeddedEntity();
            EmbeddableType embeddable = metamodel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
            Set<Attribute> embeddedAttributes = embeddable.getAttributes();
            for (Attribute embeddedAttribute : embeddedAttributes) {
                processEmbeddedField(embeddedEntity, embeddedObj, embeddedAttribute);
            }
            AzureTableUtils.setPropertyHelper(tableEntity, jpaColumnName, AzureTableUtils.serialize(embeddedEntity));
        } catch (IOException e) {
            throw new KunderaException("Some errors occurred while serializing the object: ", e);
        }
    }

    private void processEmbeddedField(EmbeddedEntity embeddedEntity, Object embeddedObj, Attribute attribute) {
        Field field = (Field) attribute.getJavaMember();
        Object valueObj = PropertyAccessorHelper.getObject(embeddedObj, field);
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        logger.debug("field = [" + field.getName() + "], jpaColumnName = [" + jpaColumnName + "], embeddedValueObj = [" + valueObj + "]");

        try {
            embeddedEntity.setProperty(jpaColumnName, AzureTableUtils.serialize(valueObj));
        } catch (IOException e) {
            throw new KunderaException("Some errors occurred while serializing the object: ", e);
        }
    }

    private void handleRelations(DynamicEntity tableEntity, EntityMetadata entityMetadata, List<RelationHolder> rlHolders) {
        if (rlHolders != null && !rlHolders.isEmpty()) {
            for (RelationHolder rh : rlHolders) {
                String jpaColumnName = rh.getRelationName();
                String fieldName = entityMetadata.getFieldName(jpaColumnName);
                Object targetId = rh.getRelationValue();

                if (jpaColumnName != null && targetId != null) {
                    // pass through AzureTableKey just for validation
                    AzureTableKey targetKey = new AzureTableKey(targetId.toString());
                    logger.debug("field = [" + fieldName + "], jpaColumnName = [" + jpaColumnName + "], targetKey = [" + targetKey.toString(true) + "]");
                    AzureTableUtils.setPropertyHelper(tableEntity, jpaColumnName, targetKey.toString(true));
                }
            }
        }
    }

    private void handleDiscriminatorColumn(DynamicEntity tableEntity, EntityType entityType) {
        String discriminatorColumn = ((AbstractManagedType) entityType).getDiscriminatorColumn();
        String discriminatorValue = ((AbstractManagedType) entityType).getDiscriminatorValue();

        if (discriminatorColumn != null && discriminatorValue != null) {
            logger.debug("discriminatorColumn = [" + discriminatorColumn + "], discriminatorValue = [" + discriminatorValue + "]");
            AzureTableUtils.setPropertyHelper(tableEntity, discriminatorColumn, discriminatorValue);
        }
    }

    /* (non-Javadoc)
     *
     * persist join table for ManyToMany
     *
     * for example:
     *  -----------------------------------------------------------------------
     *  |                    EMPLOYEE_PROJECT (joinTableName)                  |
     *  -----------------------------------------------------------------------
     *  | EMPLOYEE_ID (joinColumnName)  |  PROJECT_ID (inverseJoinColumnName)  |
     *  -----------------------------------------------------------------------
     *  |          key (owner)          |             key (child)              |
     *  -----------------------------------------------------------------------
     *
     *  note: owner and child are string representations of AzureTableKey
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String inverseJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        for (Object owner : joinTableRecords.keySet()) {
            AzureTableKey ownerKey = new AzureTableKey(owner.toString());
            Set<Object> children = joinTableRecords.get(owner);
            AzureTableKey childKey;
            for (Object child : children) {
                childKey = new AzureTableKey(child.toString());
                // partition key is the table name, row key is random generated
                DynamicEntity tableEntity = new DynamicEntity(joinTableName, UUID.randomUUID().toString());
                AzureTableUtils.setPropertyHelper(tableEntity, joinColumnName, ownerKey.toString(true));
                AzureTableUtils.setPropertyHelper(tableEntity, inverseJoinColumnName, childKey.toString(true));

                try {
                    CloudTable joinTable = tableClient.getTableReference(joinTableName);
                    if (joinTable.createIfNotExist()) {
                        logger.info("Join Table " + joinTableName + " has been created");
                    }
                    TableOperation insertOperation = TableOperation.insertOrReplace(tableEntity);
                    tableClient.execute(joinTableName, insertOperation);
                    logger.info(tableEntity.toString());
                } catch (URISyntaxException | StorageException e) {
                    throw new KunderaException("Some error occurred while persisting join table entry: ", e);
                }
            }
        }
    }

    /*---------------------------------------------------------------------------------*/
    /*------------------------------ FIND OPERATIONS ----------------------------------*/
    /*---------------------------------------------------------------------------------*/

    /* (non-Javadoc)
     *
     * it's called to find detached entities
     */
    @Override
    public Object find(Class entityClass, Object id) {
        logger.debug("entityClass = [" + entityClass.getSimpleName() + "], id = [" + id + "]");

        try {
            AzureTableKey key = new AzureTableKey(id.toString());
            EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
            DynamicEntity tableEntity = get(entityMetadata.getTableName(), key);
            if (tableEntity == null) {
                /* case not found */
                return null;
            }
            logger.info(tableEntity.toString());
            return initializeEntity(tableEntity, entityClass);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new KunderaException(e);
        }
    }

    private DynamicEntity get(String table, AzureTableKey key) {
        try {
            TableOperation retrieveOperation = TableOperation.retrieve(key.getPartitionKey(), key.getRowKey(), DynamicEntity.class);
            DynamicEntity tableEntity = tableClient.execute(table, retrieveOperation).getResultAsType();
            if (tableEntity == null) {
                logger.info("Not found {table = [" + table + "], key = [" + key.toString() + "]}");
            }
            return tableEntity;
        } catch (StorageException e) {
            throw new KunderaException("A problem occurred while retrieving the entity with key: " + key.toString(), e);
        }
    }

    private EnhanceEntity initializeEntity(DynamicEntity tableEntity, Class entityClass) throws IllegalAccessException, InstantiationException {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata, entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());
        String idAttribute = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        Map<String, Object> relationMap = new HashMap<>();
        Object entity = entityMetadata.getEntityClazz().newInstance();

        initializeID(tableEntity, entityMetadata, entity);
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attribute : attributes) {
            // ignore id attribute, handled in initializeID(...)
            if (((AbstractAttribute) attribute).getJPAColumnName().equals(idAttribute)) {
                continue;
            }
            if (!attribute.isAssociation()) {
                if (metamodel.isEmbeddable(((AbstractAttribute) attribute).getBindableJavaType())) {
                    initializeEmbeddedAttribute(tableEntity, entity, attribute, metamodel);
                } else {
                    initializeAttribute(tableEntity, entity, attribute);
                }
            } else if (!relationWillBeFilledByQuery(entityMetadata, attribute)) {
                initializeRelation(tableEntity, attribute, relationMap);
            }
        }
        logger.info(entity.toString());

        String pKey = AzureTableKey.asString(tableEntity.getPartitionKey(), tableEntity.getRowKey());
        return new EnhanceEntity(entity, pKey, relationMap.isEmpty() ? null : relationMap);
    }

    private boolean relationWillBeFilledByQuery(EntityMetadata entityMetadata, Attribute attribute) {
        Relation.ForeignKey relationType = entityMetadata.getRelation(attribute.getName()).getType();
        return relationType.equals(Relation.ForeignKey.ONE_TO_MANY) || relationType.equals(Relation.ForeignKey.MANY_TO_MANY);
    }

    private void initializeID(DynamicEntity tableEntity, EntityMetadata entityMetadata, Object entity) {
        String jpaColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        String pKey = AzureTableKey.asString(tableEntity.getPartitionKey(), tableEntity.getRowKey());

        logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + pKey + "]");
        PropertyAccessorHelper.setId(entity, entityMetadata, pKey);
    }

    private void initializeAttribute(DynamicEntity tableEntity, Object entity, Attribute attribute) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        EntityProperty entityProperty = tableEntity.getProperties().get(jpaColumnName);

        if (jpaColumnName != null && entityProperty != null) {
            Object fieldValue;
            Class<?> type = ((Field) attribute.getJavaMember()).getType();
            if (type.isEnum()) {
                EnumAccessor accessor = new EnumAccessor();
                fieldValue = accessor.fromString(((AbstractAttribute) attribute).getBindableJavaType(), entityProperty.getValueAsString());
            } else if (isCollectionOrMap(type)) {
                try {
                    fieldValue = AzureTableUtils.deserialize(entityProperty);
                } catch (ClassNotFoundException | IOException e) {
                    throw new KunderaException("Some errors occurred while deserializing the object: ", e);
                }
            } else {
                fieldValue = AzureTableUtils.getPropertyValue(entityProperty, type);
            }

            if (fieldValue != null) {
                logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue + "]");
                PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), fieldValue);
            }
        }
    }

    private boolean isCollectionOrMap(Class<?> type) {
        return Collection.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type);
    }

    private void initializeEmbeddedAttribute(DynamicEntity tableEntity, Object entity, Attribute attribute, MetamodelImpl metamodel) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        EntityProperty entityProperty = tableEntity.getProperties().get(jpaColumnName);

        if (jpaColumnName != null && entityProperty != null) {
            logger.debug("jpaColumnName = [" + jpaColumnName + "], embedded entity");

            try {
                EmbeddedEntity embeddedEntity = (EmbeddedEntity) AzureTableUtils.deserialize(entityProperty);
                EmbeddableType embeddable = metamodel.embeddable(((AbstractAttribute) attribute).getBindableJavaType());
                Object embeddedObj = embeddable.getJavaType().newInstance();
                Set<Attribute> embeddedAttributes = embeddable.getAttributes();
                for (Attribute embeddedAttribute : embeddedAttributes) {
                    initializeEmbeddedField(embeddedEntity, embeddedObj, embeddedAttribute);
                }
                PropertyAccessorHelper.set(entity, (Field) attribute.getJavaMember(), embeddedObj);
            } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | IOException e) {
                throw new KunderaException("Some errors occurred while deserializing the object: ", e);
            }
        }
    }

    private void initializeEmbeddedField(EmbeddedEntity embeddedEntity, Object embeddedObj, Attribute attribute) {
        try {
            String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
            Object fieldValue = AzureTableUtils.deserialize(embeddedEntity.getProperty(jpaColumnName));
            logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue.toString() + "]");

            PropertyAccessorHelper.set(embeddedObj, (Field) attribute.getJavaMember(), fieldValue);
        } catch (IOException | ClassNotFoundException e) {
            throw new KunderaException("Some errors occurred while deserializing the field " + attribute.getName() + ": ", e);
        }
    }

    private void initializeRelation(DynamicEntity tableEntity, Attribute attribute, Map<String, Object> relationMap) {
        String jpaColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
        EntityProperty entityProperty = tableEntity.getProperties().get(jpaColumnName);

        if (jpaColumnName != null && entityProperty != null) {
            Object fieldValue = entityProperty.getValueAsString();
            logger.debug("jpaColumnName = [" + jpaColumnName + "], fieldValue = [" + fieldValue + "]");
            // field value is a string representation of an AzureTableKey
            relationMap.put(jpaColumnName, fieldValue);
        }
    }

    /* (non-Javadoc)
     *
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

    /* (non-Javadoc)
     *
     * It can be ignored, It was in place to purely support Cassandra's super columns.
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        throw new UnsupportedOperationException("Not supported in " + this.getClass().getSimpleName());
    }

    /* (non-Javadoc)
     *
     * used to retrieve relation for OneToMany (ManyToOne inverse relation),
     * is supposed to retrieve the initialized objects.
     *
     * for example:
     *      select * from EmployeeMTObis (table name of entityClass)
     *      where DEPARTMENT_ID (colName) equals (colValue)
     *
     * note: colValue should be a string representation of AzureTableKey
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClass) {
        logger.debug("colName = [" + colName + "], colValue = [" + colValue + "], entityClazz = [" + entityClass + "]");

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        String tableName = entityMetadata.getTableName();

        AzureTableKey colValueKey = new AzureTableKey(colValue.toString());
        TableQuery<DynamicEntity> query = generateRelationQuery(tableName, colName, colValueKey.toString(true));
        List<Object> results = new ArrayList<>();
        for (DynamicEntity entity : tableClient.execute(query)) {
            String entityKey = AzureTableKey.asString(entity.getPartitionKey(), entity.getRowKey());
            results.add(find(entityClass, entityKey));
        }
        return results;
    }

    /* (non-Javadoc)
     *
     * used to retrieve owner-side relation for ManyToMany,
     * is supposed to retrieve the objects id.
     *
     * for example:
     *      select PROJECT_ID (columnName) from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (pKeyColumnName) equals (pKeyColumnValue)
     *
     * note: pKeyColumnValue and columnName should be string representation of AzureTableKey
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue, Class columnJavaType) {
        logger.debug("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyColumnName = [" + pKeyColumnName + "], columnName = [" + columnName + "], pKeyColumnValue = [" + pKeyColumnValue + "], columnJavaType = [" + columnJavaType + "]");

        AzureTableKey pKeyColumnValueKey = new AzureTableKey(pKeyColumnValue.toString());
        TableQuery<DynamicEntity> query = generateRelationQuery(tableName, pKeyColumnName, pKeyColumnValueKey.toString(true));
        List<E> results = new ArrayList<>();
        logger.debug(columnName + " for " + pKeyColumnName + "[" + pKeyColumnValue + "]:");
        for (DynamicEntity entity : tableClient.execute(query)) {
            logger.debug("\t" + entity.getProperty(columnName).getValueAsString());
            results.add((E) entity.getProperty(columnName).getValueAsString());
        }
        return results;
    }

    /* (non-Javadoc)
     *
     * used to retrieve target-side relation for ManyToMany,
     * is supposed to retrieve the objects id.
     *
     * for example:
     *      select EMPLOYEE_ID (pKeyName) from EMPLOYEE_PROJECT (tableName)
     *      where PROJECT_ID (columnName) equals (columnValue)
     *
     * note: columnValue should be a string representation of AzureTableKey
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName, Object columnValue, Class entityClazz) {
        logger.debug("schemaName = [" + schemaName + "], tableName = [" + tableName + "], pKeyName = [" + pKeyName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "], entityClazz = [" + entityClazz + "]");

        AzureTableKey columnValueKey = new AzureTableKey(columnValue.toString());
        TableQuery<DynamicEntity> query = generateRelationQuery(tableName, columnName, columnValueKey.toString(true));
        List<Object> results = new ArrayList<>();
        logger.debug(pKeyName + " for " + columnName + "[" + columnValue + "]:");
        for (DynamicEntity entity : tableClient.execute(query)) {
            // pKeyName should be a string representation of AzureTableKey
            logger.debug("\t" + entity.getProperty(pKeyName).getValueAsString());
            results.add(entity.getProperty(pKeyName).getValueAsString());
        }
        return results.toArray();
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
            DynamicEntity tableEntity = new DynamicEntity(key.getPartitionKey(), key.getRowKey());
            tableEntity.setEtag("*");
            TableOperation deleteOperation = TableOperation.delete(tableEntity);
            tableClient.execute(entityMetadata.getTableName(), deleteOperation);
        } catch (StorageException e) {
            throw new KunderaException("A problem occurred while deleting the entity: ", e);
        }
    }

    /* (non-Javadoc)
     *
     * used to delete relation for ManyToMany
     *
     * for example:
     *      delete from EMPLOYEE_PROJECT (tableName)
     *      where EMPLOYEE_ID (columnName) equals (columnValue)
     *
     * note: columnValue should be a string representation of AzureTableKey
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        logger.debug("schemaName = [" + schemaName + "], tableName = [" + tableName + "], columnName = [" + columnName + "], columnValue = [" + columnValue + "]");

        AzureTableKey columnValueKey = new AzureTableKey(columnValue.toString());
        TableQuery<DynamicEntity> query = generateRelationQuery(tableName, columnName, columnValueKey.toString(true));
        for (DynamicEntity entity : tableClient.execute(query)) {
            try {
                TableOperation deleteOperation = TableOperation.delete(entity);
                tableClient.execute(tableName, deleteOperation);
            } catch (StorageException e) {
                throw new KunderaException("A problem occurred while deleting the entity: ", e);
            }
        }
    }

    /*---------------------------------------------------------------------------------*/
    /*-------------------------------- QUERY UTILS ------------------------------------*/
    /*---------------------------------------------------------------------------------*/

    private TableQuery<DynamicEntity> generateRelationQuery(String tableName, String columnName, String targetKey) {
        logger.debug("SELECT * FROM " + tableName + " WHERE " + columnName + " = " + targetKey);
        return TableQuery.from(tableName, DynamicEntity.class)
                .where(TableQuery.generateFilterCondition(columnName, TableQuery.QueryComparisons.EQUAL, targetKey));
    }

    public List<Object> executeQuery(QueryBuilder builder) {
        logger.info(AzureTableQuery.asString(builder.getQuery()));

        int index = 0;
        List<Object> results = new ArrayList<>();
        for (DynamicEntity entity : tableClient.execute(builder.getQuery())) {
            logger.debug(entity.toString());
            try {
                EnhanceEntity ee = initializeEntity(entity, builder.getEntityClass());
                if (!builder.holdRelationships()) {
                        /* comes from AzureTableQuery.populateEntities */
                    results.add(ee.getEntity());
                } else {
                        /* comes from AzureTableQuery.recursivelyPopulateEntities */
                    results.add(ee);
                }
            } catch (InstantiationException | IllegalAccessException e) {
                throw new KunderaException(e);
            }

            /*
             * When you start iterating over result you'll initially get only X items.
             * But underneath, if you keep iterating over the result, the SDK will keep querying the table
             * (and proceed to the next 'page' of X items).
             */
            index++;
            if (index == builder.getLimit()) {
                break;
            }
        }
        return results;
    }
}
