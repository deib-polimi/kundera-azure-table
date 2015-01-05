package it.polimi.client.azuretable;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Class for embedded object, to be serialized in a Table field.
 *
 * @author Fabio Arcidiacono.
 */
public class EmbeddedEntity implements Serializable {

    private HashMap<String, byte[]> properties = new HashMap<>();

    public EmbeddedEntity() {}

    public void setProperty(String name, byte[] value) {
        this.properties.put(name, value);
    }

    public byte[] getProperty(String name) {
        return this.properties.get(name);
    }
}
