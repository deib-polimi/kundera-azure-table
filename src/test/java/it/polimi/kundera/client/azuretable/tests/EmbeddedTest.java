package it.polimi.kundera.client.azuretable.tests;

import it.polimi.kundera.client.azuretable.entities.Address;
import it.polimi.kundera.client.azuretable.entities.EmployeeEmbedded;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Fabio Arcidiacono.
 */
public class EmbeddedTest extends TestBase {

    @Test
    public void testEmbedded() {
        print("create");
        EmployeeEmbedded employee = new EmployeeEmbedded();
        employee.setName("Fabio");
        employee.setSalary(123L);
        Address address = new Address("Via Cadore 12");
        employee.setAddress(address);
        em.persist(employee);
        Assert.assertNotNull(employee.getId());

        String empId = employee.getId();
        clear();

        print("read");
        EmployeeEmbedded foundEmployee = em.find(EmployeeEmbedded.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getAddress());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertTrue(foundEmployee.getAddress() instanceof Address);
        Assert.assertEquals("Via Cadore 12", foundEmployee.getAddress().getStreet());

        print("update");
        foundEmployee.getAddress().setStreet("Piazza Leonardo Da Vinci 32");
        em.merge(foundEmployee);

        clear();

        foundEmployee = em.find(EmployeeEmbedded.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getAddress());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertTrue(foundEmployee.getAddress() instanceof Address);
        Assert.assertEquals("Piazza Leonardo Da Vinci 32", foundEmployee.getAddress().getStreet());

        print("delete");
        em.remove(foundEmployee);
        foundEmployee = em.find(EmployeeEmbedded.class, empId);
        Assert.assertNull(foundEmployee);
    }
}
