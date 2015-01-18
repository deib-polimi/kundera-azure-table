package it.polimi.kundera.client.azuretable;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.AbstractEntityReader;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.query.KunderaQuery;

import java.util.List;

/**
 * Used by Kundera to translate the queries into correct client method calls.
 *
 * @author Fabio Arcidiacono.
 * @see com.impetus.kundera.persistence.AbstractEntityReader
 * @see com.impetus.kundera.persistence.EntityReader
 */
public class AzureTableEntityReader extends AbstractEntityReader implements EntityReader {

    public AzureTableEntityReader(final KunderaMetadata kunderaMetadata) {
        super(kunderaMetadata);
    }

    public AzureTableEntityReader(KunderaQuery kunderaQuery, final KunderaMetadata kunderaMetadata) {
        super(kunderaMetadata);
        this.kunderaQuery = kunderaQuery;
    }

    /*
     * This is used by Query implementor to populate relationship entities into their parent entity.
     */
    @Override
    public List<EnhanceEntity> populateRelation(EntityMetadata m, Client client, int maxResults) {
        throw new UnsupportedOperationException("Method not required for Azure Table");
    }

    @Override
    public EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client) {
        return super.findById(primaryKey, m, client);
    }
}
