package it.polimi.client.azuretable;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.microsoft.windowsazure.services.table.client.EntityProperty;

import java.io.*;
import java.util.Date;
import java.util.UUID;

/**
 * Utils method for common operation with Table api.
 *
 * @author Fabio Arcidiacono.
 */
public class AzureTableUtils {

    /**
     * Generate a {@link it.polimi.client.azuretable.DynamicEntity} from
     * {@link com.impetus.kundera.metadata.model.EntityMetadata}.
     *
     * @param entityMetadata metadata from Kundera ({@link com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata}).
     * @param id             entity id.
     * @return a fresh new  {@link it.polimi.client.azuretable.DynamicEntity}
     * @throws com.impetus.kundera.KunderaException if id is not of type {@link String}.
     * @see it.polimi.client.azuretable.DynamicEntity
     */
    public static DynamicEntity createDynamicEntity(EntityMetadata entityMetadata, Object id) {
        Class idClazz = entityMetadata.getIdAttribute().getJavaType();
        if (!(idClazz.equals(String.class))) {
            throw new KunderaException("Id attribute must be of type " + String.class);
        }
        return createDynamicEntity((String) id);
    }

    /**
     * Generate a {@link it.polimi.client.azuretable.DynamicEntity}.
     *
     * @param id string representation of {@link it.polimi.client.azuretable.AzureTableKey}.
     * @return a fresh new  {@link it.polimi.client.azuretable.DynamicEntity}
     * @see it.polimi.client.azuretable.AzureTableKey
     * @see it.polimi.client.azuretable.DynamicEntity
     */
    public static DynamicEntity createDynamicEntity(String id) {
        AzureTableKey key = new AzureTableKey(id);
        return new DynamicEntity(key.getPartitionKey(), key.getRowKey());
    }

    /**
     * An helper method to set a property to an instance of {@link it.polimi.client.azuretable.DynamicEntity}.
     *
     * @param tableEntity   injected entity
     * @param jpaColumnName property name
     * @param value         property value
     */
    public static void setPropertyHelper(DynamicEntity tableEntity, String jpaColumnName, Object value) {
        tableEntity.setProperty(jpaColumnName, getEntityProperty(value));
    }

    /**
     * Serialize an object into a byte[].
     *
     * @param obj object to be serialized.
     * @return a byte[] containing the serialization.
     * @throws java.io.IOException
     */
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    /**
     * Deserialize an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param property a datastore {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     * @return the deserialized object.
     * @throws java.io.IOException
     * @throws ClassNotFoundException
     * @see com.microsoft.windowsazure.services.table.client.EntityProperty
     */
    public static Object deserialize(EntityProperty property) throws IOException, ClassNotFoundException {
        byte[] bytes = property.getValueAsByteArray();
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }

    /**
     * Generate an instance of {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param value the value to be wrapped into an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}
     * @return an instance of {@link com.microsoft.windowsazure.services.table.client.EntityProperty} for the given object.
     * @throws com.impetus.kundera.KunderaException if type is not supported by AzureTable.
     */
    public static EntityProperty getEntityProperty(Object value) {
        if (value instanceof String) {
            return new EntityProperty((String) value);
        }
        if (value instanceof Double) {
            return new EntityProperty((Double) value);
        }
        if (value instanceof Integer) {
            return new EntityProperty((Integer) value);
        }
        if (value instanceof Long) {
            return new EntityProperty((Long) value);
        }
        if (value instanceof Boolean) {
            return new EntityProperty((Boolean) value);
        }
        if (value instanceof byte[]) {
            return new EntityProperty((byte[]) value);
        }
        if (value instanceof Byte[]) {
            return new EntityProperty((Byte[]) value);
        }
        if (value instanceof Date) {
            return new EntityProperty((Date) value);
        }
        if (value instanceof UUID) {
            return new EntityProperty((UUID) value);
        }
        throw new KunderaException("Unsupported type " + value.getClass().getCanonicalName());
    }

    /**
     * Retrieve the property value from an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param entityProperty the property container
     * @param type           type class of the property to be retrieved
     * @return the retrieved property
     */
    public static Object getPropertyValue(EntityProperty entityProperty, Class<?> type) {
        if (String.class.equals(type)) {
            return entityProperty.getValueAsString();
        }
        if (Double.class.equals(type)) {
            return entityProperty.getValueAsDouble();
        }
        if (Integer.class.equals(type)) {
            return entityProperty.getValueAsInteger();
        }
        if (Long.class.equals(type)) {
            return entityProperty.getValueAsLong();
        }
        if (Boolean.class.equals(type)) {
            return entityProperty.getValueAsBoolean();
        }
        if (byte[].class.equals(type)) {
            return entityProperty.getValueAsByteArray();
        }
        if (Byte[].class.equals(type)) {
            return entityProperty.getValueAsByteObjectArray();
        }
        if (Date.class.equals(type)) {
            return entityProperty.getValueAsDate();
        }
        if (UUID.class.equals(type)) {
            return entityProperty.getValueAsUUID();
        }
        throw new KunderaException("Unknown type " + type);
    }
}
