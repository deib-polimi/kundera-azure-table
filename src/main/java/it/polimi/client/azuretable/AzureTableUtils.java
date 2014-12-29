package it.polimi.client.azuretable;

import com.microsoft.windowsazure.services.table.client.EntityProperty;

import java.io.*;

/**
 * Utils method for common operation with Table api.
 *
 * @author Fabio Arcidiacono.
 */
public class AzureTableUtils {

    /**
     * Serialize an object into an {@link com.microsoft.windowsazure.services.table.client.EntityProperty}.
     *
     * @param obj object to be serialized.
     *
     * @return an instance of {@link com.microsoft.windowsazure.services.table.client.EntityProperty} of the given object.
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
