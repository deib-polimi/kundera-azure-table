package it.polimi.kundera.client.azuretable.tests;

import it.polimi.kundera.client.azuretable.entities.AddressCollection;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Fabio Arcidiacono.
 */
public class ElementCollectionTest extends TestBase {

    @Test
    public void testElementCollection() {
        print("create");
        AddressCollection address = new AddressCollection();
        address.setStreets("Street 1", "Street 2", "Street 3");
        em.persist(address);
        Assert.assertNotNull(address.getId());

        String adrId = address.getId();
        clear();

        print("read");
        AddressCollection foundAddress = em.find(AddressCollection.class, adrId);
        Assert.assertNotNull(foundAddress);
        Assert.assertNotNull(foundAddress.getStreets());
        Assert.assertEquals(adrId, foundAddress.getId());
        Assert.assertFalse(foundAddress.getStreets().isEmpty());
        Assert.assertFalse(foundAddress.getStreets().size() > 3);
        print("access streets");
        int counter = 3;
        for (String street : foundAddress.getStreets()) {
            if (street.equals("Street 1") || street.equals("Street 2") || street.equals("Street 3")) {
                counter--;
            }
        }
        Assert.assertEquals(0, counter);

        print("update");
        foundAddress.setStreets("Street 4", "Street 5", "Street 6");
        em.merge(foundAddress);

        clear();

        foundAddress = em.find(AddressCollection.class, adrId);
        Assert.assertNotNull(foundAddress);
        Assert.assertNotNull(foundAddress.getStreets());
        Assert.assertEquals(adrId, foundAddress.getId());
        Assert.assertFalse(foundAddress.getStreets().isEmpty());
        Assert.assertFalse(foundAddress.getStreets().size() > 3);
        print("access streets");
        counter = 3;
        for (String street : foundAddress.getStreets()) {
            if (street.equals("Street 4") || street.equals("Street 5") || street.equals("Street 6")) {
                counter--;
            }
        }
        Assert.assertEquals(0, counter);

        print("delete");
        em.remove(foundAddress);
        foundAddress = em.find(AddressCollection.class, adrId);
        Assert.assertNull(foundAddress);
    }
}
