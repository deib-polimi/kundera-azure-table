package it.polimi.client.azuretable;

import com.microsoft.windowsazure.services.table.client.DynamicTableEntity;
import com.microsoft.windowsazure.services.table.client.EntityProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * This class lets you dynamically map java properties to AzureTables not requiring static POJO.
 *
 * @author Marco Scavuzzo.
 * @author Fabio Arcidiacono.
 * @see it.polimi.client.azuretable.DynamicEntity
 * @see com.microsoft.windowsazure.services.table.client.EntityProperty
 */
public class DynamicEntity extends DynamicTableEntity {

    private HashMap<String, EntityProperty> properties = new HashMap<>();

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
        this.properties.put(name, entityProperty);
        super.setProperties(properties);
    }

    @Override
    public String toString() {
        String key = partitionKey + "(" + rowKey + ")";
        String properties = "";
        for (Map.Entry<String, EntityProperty> entry : getProperties().entrySet()) {
            properties += entry.getKey() + " = " + entry.getValue().toString() + "\n";
        }
        return "<" + this.getClass().getSimpleName() + "[" + key + "]:\n" + properties + ">";
    }
}
