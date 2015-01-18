package it.polimi.kundera.client.azuretable;

import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * This class lets you dynamically map java properties to AzureTables not requiring static POJO.
 *
 * @author Marco Scavuzzo.
 * @author Fabio Arcidiacono.
 * @see DynamicEntity
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

    public void setProperty(String name, EntityProperty entityProperty) {
        super.getProperties().put(name, entityProperty);
    }

    public EntityProperty getProperty(String name) {
        return super.getProperties().get(name);
    }

    @Override
    public String toString() {
        String key = partitionKey + "(" + rowKey + ")";
        String properties = "";
        for (Map.Entry<String, EntityProperty> entry : getProperties().entrySet()) {
            properties += "\t" + entry.getKey() + " = " + entry.getValue().getValueAsString() + "\n";
        }
        return "<" + this.getClass().getSimpleName() + "[" + key + "]:\n" + properties + ">\n";
    }
}
