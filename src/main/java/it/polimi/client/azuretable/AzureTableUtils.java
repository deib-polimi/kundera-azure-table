package it.polimi.client.azuretable;

import com.impetus.kundera.KunderaException;
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
     * Generate an instance of {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param value the value to be wrapped into an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}
     *
     * @return an instance of {@link com.microsoft.windowsazure.services.table.client.EntityProperty} for the given object.
     *
     * @throws com.impetus.kundera.KunderaException if type is not supported by AzureTable.
     */
    public static EntityProperty getEntityProperty(Object value) {
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
        if (value instanceof Double) {
            return new EntityProperty((Double) value);
        }
        if (value instanceof Integer) {
            return new EntityProperty((Integer) value);
        }
        if (value instanceof Long) {
            return new EntityProperty((Long) value);
        }
        if (value instanceof String) {
            return new EntityProperty((String) value);
        }
        if (value instanceof UUID) {
            return new EntityProperty((UUID) value);
        }
        throw new KunderaException("Unsupported type " + value.getClass().getCanonicalName());
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
     * Serialize an object into an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param obj object to be serialized.
     *
     * @return an instance of {@link com.microsoft.windowsazure.services.table.client.EntityProperty} for the given object.
     *
     * @throws java.io.IOException
     * @see com.microsoft.windowsazure.services.table.client.EntityProperty
     */
    public static EntityProperty serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return new EntityProperty(b.toByteArray());
    }

    /**
     * Deserialize an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param property a datastore {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @return the deserialized object.
     *
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
}
