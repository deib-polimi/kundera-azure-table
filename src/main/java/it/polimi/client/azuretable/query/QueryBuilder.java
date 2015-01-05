package it.polimi.client.azuretable.query;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.query.KunderaQuery;
import com.microsoft.windowsazure.services.table.client.TableQuery;
import it.polimi.client.azuretable.AzureTableKey;
import it.polimi.client.azuretable.DynamicEntity;

import javax.persistence.metamodel.EntityType;
import java.util.*;

/**
 * Helpful methods to translate from {@link com.impetus.kundera.query.KunderaQuery}
 * to {@link com.microsoft.windowsazure.services.table.client.TableQuery}.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.query.KunderaQuery
 * @see com.microsoft.windowsazure.services.table.client.TableQuery
 */
public class QueryBuilder {

    private TableQuery<DynamicEntity> query;
    private final EntityType entityType;
    private final EntityMetadata entityMetadata;
    /** true if {@link javax.persistence.metamodel.EntityType} of the query holds relationships with other entities */
    private boolean holdRelationships;
    /** true if the select query is different from "SELECT *" */
    private boolean isProjectionQuery;
    private Map<String, Class> projections;

    public QueryBuilder(EntityMetadata entityMetadata, EntityType entityType, boolean holdRelationships) {
        this.entityMetadata = entityMetadata;
        this.entityType = entityType;
        this.holdRelationships = holdRelationships;
        this.isProjectionQuery = false;
        this.projections = new HashMap<>();
    }

    public TableQuery<DynamicEntity> getQuery() {
        return this.query;
    }

    public Class getEntityClass() {
        return this.entityMetadata.getEntityClazz();
    }

    public boolean holdRelationships() {
        return this.holdRelationships;
    }

    public boolean isProjectionQuery() {
        return this.isProjectionQuery;
    }

    public Set<String> getProjections() {
        return this.projections.keySet();
    }

    public int getLimit() {
        return query.getTakeCount();
    }

    /**
     * Initialize a new {@link com.microsoft.windowsazure.services.table.client.TableQuery} on the given table.
     *
     * @param tableName table subject of the query
     *
     * @return this, for chaining.
     */
    public QueryBuilder setFrom(String tableName) {
        this.query = TableQuery.from(tableName, DynamicEntity.class);
        return this;
    }

    /**
     * Set a limit to the query.
     *
     * @param limit an {@code int} limit value.
     *
     * @return this, for chaining.
     */
    public QueryBuilder setLimit(int limit) {
        this.query.take(limit);
        return this;
    }

//    /**
//     * Add multiple projections to the query.
//     *
//     * @param columns array of column names on which add a projection.
//     *
//     * @return this, for chaining.
//     *
//     * @throws com.impetus.kundera.KunderaException if Java type is not found for a column.
//     * @see com.google.appengine.api.datastore.PropertyProjection
//     */
//    public QueryBuilder addProjections(String[] columns) {
//        if (columns.length != 0) {
//            this.isProjectionQuery = true;
//            for (String column : columns) {
//                try {
//                    String filedName = entityMetadata.getFieldName(column);
//                    Attribute attribute = entityType.getAttribute(filedName);
//                    addProjection(column, attribute.getJavaType());
//                } catch (NullPointerException e) {
//                    /* case attribute not found */
//                    throw new KunderaException("Cannot find Java type for " + column + ": ", e);
//                }
//            }
//        }
//        return this;
//    }
//
//    /**
//     * Add a projection to the query.
//     *
//     * @param column     the column name.
//     * @param columnType Java type of the column.
//     *
//     * @return this, for chaining.
//     *
//     * @see com.google.appengine.api.datastore.PropertyProjection
//     */
//    public QueryBuilder addProjection(String column, Class columnType) {
//        this.projections.put(column, columnType);
//        this.query.addProjection(new PropertyProjection(column, columnType));
//        return this;
//    }

    /**
     * Add multiple filters to the query.
     *
     * @param filterClauseQueue filter clause queue from {@link com.impetus.kundera.query.KunderaQuery}.
     *
     * @return this, for chaining.
     *
     * @see com.impetus.kundera.query.KunderaQuery.FilterClause
     */
    public QueryBuilder addFilters(Queue filterClauseQueue) {
        boolean isComposite = false;
        String composeOperator = null;
        String previousFilter = null;

        for (Object filterClause : filterClauseQueue) {
            if (filterClause instanceof KunderaQuery.FilterClause) {
                String propertyFilter = generatePropertyFilter((KunderaQuery.FilterClause) filterClause);
                if (!isComposite) {
                    addFilter(propertyFilter);
                } else {
                    propertyFilter = composeFilter(propertyFilter, composeOperator, previousFilter);
                    addFilter(propertyFilter);
                }
                previousFilter = propertyFilter;
            } else if (filterClause instanceof String) {
                isComposite = true;
                composeOperator = filterClause.toString().trim();
            }
        }
        return this;
    }

    /**
     * Add a filters to the query.
     *
     * @param propertyFilter azure table filter string.
     *
     * @return this, for chaining.
     */
    public QueryBuilder addFilter(String propertyFilter) {
        this.query.where(propertyFilter);
        return this;
    }

    private String composeFilter(String propertyFilter, String composeOperator, String previousFilter) {
        if (composeOperator.equalsIgnoreCase("AND")) {
            return TableQuery.combineFilters(previousFilter, TableQuery.Operators.AND, propertyFilter);
        } else if (composeOperator.equalsIgnoreCase("OR")) {
            return TableQuery.combineFilters(previousFilter, TableQuery.Operators.OR, propertyFilter);
        }
        throw new KunderaException("Composition with " + composeOperator + " is not supported by Azure Table");
    }

    private String generatePropertyFilter(KunderaQuery.FilterClause filterClause) {
        Object filterValue = filterClause.getValue().get(0);
        String operator = parseCondition(filterClause.getCondition());
        String property = filterClause.getProperty();

        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        String filedName = entityMetadata.getFieldName(property);
        if (entityType.getAttribute(filedName).isAssociation()) {
            /* filter on related entity */
            AzureTableKey key = new AzureTableKey(filterValue.toString());
            return TableQuery.generateFilterCondition(property, operator, key.toString());
        } else if (property.equals(idColumnName)) {
            /* filter on entity ID */
            AzureTableKey key = new AzureTableKey(filterValue.toString());
            return generateKeyFilter(key);
        } else {
            /* filter on entity filed */
            return generateFilterCondition(property, operator, filterValue);
        }
    }

    private String generateKeyFilter(AzureTableKey key) {
        return TableQuery.combineFilters(
                TableQuery.generateFilterCondition("PartitionKey", TableQuery.QueryComparisons.EQUAL, key.getPartitionKey()),
                TableQuery.Operators.AND,
                TableQuery.generateFilterCondition("RowKey", TableQuery.QueryComparisons.EQUAL, key.getRowKey()));
    }

    private String generateFilterCondition(String property, String operator, Object filterValue) {
        if (filterValue instanceof String) {
            return TableQuery.generateFilterCondition(property, operator, (String) filterValue);
        }
        if (filterValue instanceof Double) {
            return TableQuery.generateFilterCondition(property, operator, (Double) filterValue);
        }
        if (filterValue instanceof Integer) {
            return TableQuery.generateFilterCondition(property, operator, (Integer) filterValue);
        }
        if (filterValue instanceof Long) {
            return TableQuery.generateFilterCondition(property, operator, (Long) filterValue);
        }
        if (filterValue instanceof Boolean) {
            return TableQuery.generateFilterCondition(property, operator, (Boolean) filterValue);
        }
        if (filterValue instanceof byte[]) {
            return TableQuery.generateFilterCondition(property, operator, (byte[]) filterValue);
        }
        if (filterValue instanceof Byte[]) {
            return TableQuery.generateFilterCondition(property, operator, (Byte[]) filterValue);
        }
        if (filterValue instanceof Date) {
            return TableQuery.generateFilterCondition(property, operator, (Date) filterValue);
        }
        if (filterValue instanceof UUID) {
            return TableQuery.generateFilterCondition(property, operator, (UUID) filterValue);
        }
        throw new KunderaException("Unsupported type " + filterValue.getClass().getCanonicalName());
    }

    private String parseCondition(String condition) {
        switch (condition) {
            case "=":
                return TableQuery.QueryComparisons.EQUAL;
            case "!=":
                return TableQuery.QueryComparisons.NOT_EQUAL;
            case ">":
                return TableQuery.QueryComparisons.GREATER_THAN;
            case ">=":
                return TableQuery.QueryComparisons.GREATER_THAN_OR_EQUAL;
            case "<":
                return TableQuery.QueryComparisons.LESS_THAN;
            case "<=":
                return TableQuery.QueryComparisons.LESS_THAN_OR_EQUAL;
        }
        /* BETWEEN is automatically converted in (X >= K1 AND X <= K2) by Kundera */
        throw new KunderaException("Condition " + condition + " is not supported by Azure Table");
    }
}
