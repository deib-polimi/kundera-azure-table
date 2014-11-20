package it.polimi.client.azuretable.tests;

import it.polimi.client.azuretable.entities.EmployeeOTO;
import it.polimi.client.azuretable.entities.Phone;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.TypedQuery;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTableOTOTest extends TestBase {

    @Test
    public void testCRUD() {
        print("create");
        Phone phone = new Phone();
        phone.setNumber(123456789L);
        em.persist(phone);
        Assert.assertNotNull(phone.getId());

        EmployeeOTO employee = new EmployeeOTO();
        employee.setName("Fabio");
        employee.setSalary(123L);
        employee.setPhone(phone);
        em.persist(employee);
        Assert.assertNotNull(employee.getId());

        String empId = employee.getId();
        String phnId = phone.getId();
        clear();

        print("read");
        EmployeeOTO foundEmployee = em.find(EmployeeOTO.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getPhone());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertEquals(phnId, foundEmployee.getPhone().getId());
        Assert.assertEquals((Long) 123456789L, foundEmployee.getPhone().getNumber());

        print("update");
        foundEmployee.setName("Pippo");
        foundEmployee.setSalary(456L);
        foundEmployee.getPhone().setNumber(987654321L);
        em.merge(foundEmployee);

        clear();

        foundEmployee = em.find(EmployeeOTO.class, empId);
        Assert.assertNotNull(foundEmployee);
        Assert.assertNotNull(foundEmployee.getPhone());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Pippo", foundEmployee.getName());
        Assert.assertEquals((Long) 456L, foundEmployee.getSalary());
        Assert.assertEquals(phnId, foundEmployee.getPhone().getId());
        Assert.assertEquals((Long) 987654321L, foundEmployee.getPhone().getNumber());

        print("delete");
        em.remove(foundEmployee);
        foundEmployee = em.find(EmployeeOTO.class, empId);
        Assert.assertNull(foundEmployee);
    }

    @Test
    public void testQuery() {
        print("create");
        Phone phone = new Phone();
        phone.setNumber(123456789L);
        em.persist(phone);
        Assert.assertNotNull(phone.getId());

        EmployeeOTO employee = new EmployeeOTO();
        employee.setName("Fabio");
        employee.setSalary(123L);
        employee.setPhone(phone);
        em.persist(employee);
        Assert.assertNotNull(employee.getId());

        String empId = employee.getId();
        String phnId = phone.getId();
        clear();

        print("select all");
        TypedQuery<EmployeeOTO> query = em.createQuery("SELECT e FROM EmployeeOTO e", EmployeeOTO.class);
        List<EmployeeOTO> allEmployees = query.getResultList();
        int toCheck = 1;
        for (EmployeeOTO emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            Assert.assertNotNull(emp.getPhone());
            if (emp.getId().equals(empId)) {
                toCheck--;
                Assert.assertEquals(empId, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                Assert.assertEquals(phnId, emp.getPhone().getId());
                Assert.assertEquals((Long) 123456789L, emp.getPhone().getNumber());
            }
        }
        Assert.assertEquals(0, toCheck);

        clear();

        print("select by inner filed");
        query = em.createQuery("SELECT e FROM EmployeeOTO e WHERE e.phone = :pid", EmployeeOTO.class);
        EmployeeOTO foundEmployee = query.setParameter("pid", phnId).getSingleResult();
        Assert.assertNotNull(foundEmployee.getId());
        Assert.assertNotNull(foundEmployee.getPhone());
        Assert.assertEquals(empId, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertEquals(phnId, foundEmployee.getPhone().getId());
        Assert.assertEquals((Long) 123456789L, foundEmployee.getPhone().getNumber());
    }
}
