package it.polimi.client.azuretable;

import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;

import java.util.HashMap;

/**
 * This class lets you dynamically map java properties to AzureTables not requiring static POJO.
 *
 * @author Marco Scavuzzo
 * @see it.polimi.client.azuretable.DynamicEntity
 * @see com.microsoft.windowsazure.services.table.client.EntityProperty
 */
public class DynamicEntity extends DynamicTableEntity {

    public DynamicEntity() {
    }

    public DynamicEntity(String partitionKey, String rowKey) {
        super.setPartitionKey(partitionKey);
        super.setRowKey(rowKey);
    }

    public DynamicEntity(String partitionKey, String rowKey, HashMap<String, EntityProperty> properties) {
        super(properties);
        super.setPartitionKey(partitionKey);
        super.setRowKey(rowKey);
    }
}
