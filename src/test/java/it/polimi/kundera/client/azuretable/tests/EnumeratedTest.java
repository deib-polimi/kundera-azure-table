package it.polimi.kundera.client.azuretable.tests;

import it.polimi.kundera.client.azuretable.entities.PhoneEnum;
import it.polimi.kundera.client.azuretable.entities.PhoneType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Fabio Arcidiacono.
 */
public class EnumeratedTest extends TestBase {

    @Test
    public void testEnum() {
        print("create");
        PhoneEnum phone = new PhoneEnum();
        phone.setNumber(123L);
        phone.setType(PhoneType.HOME);
        em.persist(phone);
        Assert.assertNotNull(phone.getId());

        String phnId = phone.getId();
        clear();

        print("read");
        PhoneEnum foundPhone = em.find(PhoneEnum.class, phnId);
        Assert.assertNotNull(foundPhone);
        Assert.assertEquals(phnId, foundPhone.getId());
        Assert.assertEquals((Long) 123L, foundPhone.getNumber());
        Assert.assertEquals(PhoneType.HOME, foundPhone.getType());

        print("update");
        foundPhone.setType(PhoneType.MOBILE);
        em.merge(foundPhone);

        clear();

        foundPhone = em.find(PhoneEnum.class, phnId);
        Assert.assertNotNull(foundPhone);
        Assert.assertEquals(phnId, foundPhone.getId());
        Assert.assertEquals((Long) 123L, foundPhone.getNumber());
        Assert.assertEquals(PhoneType.MOBILE, foundPhone.getType());

        print("delete");
        em.remove(foundPhone);
        foundPhone = em.find(PhoneEnum.class, phnId);
        Assert.assertNull(foundPhone);
    }
}
