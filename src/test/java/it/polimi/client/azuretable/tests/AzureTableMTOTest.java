package it.polimi.client.azuretable.tests;

import it.polimi.client.azuretable.entities.Department;
import it.polimi.client.azuretable.entities.EmployeeMTO;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.TypedQuery;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTableMTOTest extends TestBase {

    @Test
    public void testCRUD() {
        print("create");
        Department department = new Department();
        department.setName("Computer Science");
        em.persist(department);
        Assert.assertNotNull(department.getId());

        EmployeeMTO employee1 = new EmployeeMTO();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.setDepartment(department);
        em.persist(employee1);
        Assert.assertNotNull(employee1.getId());

        EmployeeMTO employee2 = new EmployeeMTO();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        employee2.setDepartment(department);
        em.persist(employee2);
        Assert.assertNotNull(employee2.getId());

        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        String depId = department.getId();
        clear();

        print("read");
        print("employee 1");
        EmployeeMTO foundEmployee1 = em.find(EmployeeMTO.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertNotNull(foundEmployee1.getDepartment());
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Fabio", foundEmployee1.getName());
        Assert.assertEquals((Long) 123L, foundEmployee1.getSalary());
        Assert.assertEquals(depId, foundEmployee1.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee1.getDepartment().getName());

        print("employee 2");
        EmployeeMTO foundEmployee2 = em.find(EmployeeMTO.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertNotNull(foundEmployee2.getDepartment());
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        Assert.assertEquals(depId, foundEmployee2.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee2.getDepartment().getName());

        print("update");
        foundEmployee1.setName("Pippo");
        foundEmployee1.setSalary(456L);
        foundEmployee2.setName("Minnie");
        foundEmployee2.setSalary(789L);
        em.merge(foundEmployee1);
        em.merge(foundEmployee2);

        clear();

        print("employee 1");
        foundEmployee1 = em.find(EmployeeMTO.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertNotNull(foundEmployee1.getDepartment());
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Pippo", foundEmployee1.getName());
        Assert.assertEquals((Long) 456L, foundEmployee1.getSalary());
        Assert.assertEquals(depId, foundEmployee1.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee1.getDepartment().getName());

        print("employee 2");
        foundEmployee2 = em.find(EmployeeMTO.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertNotNull(foundEmployee2.getDepartment());
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Minnie", foundEmployee2.getName());
        Assert.assertEquals((Long) 789L, foundEmployee2.getSalary());
        Assert.assertEquals(depId, foundEmployee2.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee2.getDepartment().getName());

        print("delete");
        em.remove(foundEmployee1);
        em.remove(foundEmployee2);
        foundEmployee1 = em.find(EmployeeMTO.class, emp1Id);
        foundEmployee2 = em.find(EmployeeMTO.class, emp2Id);
        Assert.assertNull(foundEmployee1);
        Assert.assertNull(foundEmployee2);
    }

    @Test
    public void testQuery() {
        print("create");
        Department department = new Department();
        department.setName("Computer Science");
        em.persist(department);
        Assert.assertNotNull(department.getId());

        EmployeeMTO employee1 = new EmployeeMTO();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.setDepartment(department);
        em.persist(employee1);
        Assert.assertNotNull(employee1.getId());

        EmployeeMTO employee2 = new EmployeeMTO();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        employee2.setDepartment(department);
        em.persist(employee2);
        Assert.assertNotNull(employee2.getId());

        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        String depId = department.getId();
        clear();

        print("select all");
        TypedQuery<EmployeeMTO> query = em.createQuery("SELECT e FROM EmployeeMTO e", EmployeeMTO.class);
        List<EmployeeMTO> allEmployees = query.getResultList();
        int toCheck = 2;
        for (EmployeeMTO emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            Assert.assertNotNull(emp.getDepartment());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            }
        }
        Assert.assertEquals(0, toCheck);

        clear();

        print("select by inner filed");
        query = em.createQuery("SELECT e FROM EmployeeMTO e WHERE e.department = :did AND e.name = :n", EmployeeMTO.class);
        EmployeeMTO foundEmployee = query.setParameter("did", depId).setParameter("n", "Fabio").getSingleResult();
        Assert.assertNotNull(foundEmployee.getId());
        Assert.assertNotNull(foundEmployee.getDepartment());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());
        Assert.assertEquals(depId, foundEmployee.getDepartment().getId());
        Assert.assertEquals("Computer Science", foundEmployee.getDepartment().getName());
    }
}
