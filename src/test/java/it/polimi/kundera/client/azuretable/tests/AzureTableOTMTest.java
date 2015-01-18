package it.polimi.kundera.client.azuretable.tests;

import it.polimi.kundera.client.azuretable.entities.DepartmentOTM;
import it.polimi.kundera.client.azuretable.entities.EmployeeMTObis;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.TypedQuery;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTableOTMTest extends TestBase {

    @Test
    public void testCRUD() {
        print("create");
        DepartmentOTM department = new DepartmentOTM();
        department.setName("Computer Science");
        em.persist(department);
        Assert.assertNotNull(department.getId());

        EmployeeMTObis employee1 = new EmployeeMTObis();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.setDepartment(department);
        em.persist(employee1);
        Assert.assertNotNull(employee1.getId());

        EmployeeMTObis employee2 = new EmployeeMTObis();
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
        DepartmentOTM foundDepartment = em.find(DepartmentOTM.class, depId);
        Assert.assertNotNull(foundDepartment);
        print("access employees");
        int counter = 2;
        for (EmployeeMTObis emp : foundDepartment.getEmployees()) {
            Assert.assertNotNull(emp);
            Assert.assertEquals(depId, emp.getDepartment().getId());
            Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            if (emp.getId().equals(emp1Id)) {
                counter--;
                Assert.assertEquals(emp1Id, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                counter--;
                Assert.assertEquals(emp2Id, emp.getId());
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, counter);

        print("update");
        foundDepartment.setName("Software Engineering");
        em.merge(foundDepartment);

        clear();

        foundDepartment = em.find(DepartmentOTM.class, depId);
        Assert.assertNotNull(foundDepartment);
        print("access employees");
        counter = 2;
        for (EmployeeMTObis emp : foundDepartment.getEmployees()) {
            Assert.assertNotNull(emp);
            Assert.assertEquals(depId, emp.getDepartment().getId());
            Assert.assertEquals("Software Engineering", emp.getDepartment().getName());
            if (emp.getId().equals(emp1Id)) {
                counter--;
                Assert.assertEquals(emp1Id, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                counter--;
                Assert.assertEquals(emp2Id, emp.getId());
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, counter);

        print("delete");
        em.remove(foundDepartment);
        foundDepartment = em.find(DepartmentOTM.class, emp1Id);
        Assert.assertNull(foundDepartment);
    }

    @Test
    public void testQuery() {
        print("create");
        DepartmentOTM department = new DepartmentOTM();
        department.setName("Computer Science");
        em.persist(department);
        Assert.assertNotNull(department.getId());

        EmployeeMTObis employee1 = new EmployeeMTObis();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.setDepartment(department);
        em.persist(employee1);
        Assert.assertNotNull(employee1.getId());

        EmployeeMTObis employee2 = new EmployeeMTObis();
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
        TypedQuery<DepartmentOTM> query = em.createQuery("SELECT d FROM DepartmentOTM d", DepartmentOTM.class);
        List<DepartmentOTM> allDepartments = query.getResultList();
        print("access employees");
        int toCheck = 1;
        int empInDep = 2;
        for (DepartmentOTM dep : allDepartments) {
            toCheck--;
            Assert.assertEquals("Computer Science", dep.getName());
            for (EmployeeMTObis emp : dep.getEmployees()) {
                Assert.assertNotNull(emp);
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
                if (emp.getId().equals(emp1Id)) {
                    empInDep--;
                    Assert.assertEquals(emp1Id, emp.getId());
                    Assert.assertEquals("Fabio", emp.getName());
                    Assert.assertEquals((Long) 123L, emp.getSalary());
                } else if (emp.getId().equals(emp2Id)) {
                    empInDep--;
                    Assert.assertEquals(emp2Id, emp.getId());
                    Assert.assertEquals("Crizia", emp.getName());
                    Assert.assertEquals((Long) 456L, emp.getSalary());
                }
            }
        }
        Assert.assertEquals(0, toCheck);
        Assert.assertEquals(0, empInDep);

        clear();

        print("fill relation by query");
        TypedQuery<EmployeeMTObis> employeeQuery = em.createQuery("SELECT e FROM EmployeeMTObis e WHERE e.department = :did", EmployeeMTObis.class);
        List<EmployeeMTObis> depEmployees = employeeQuery.setParameter("did", depId).getResultList();
        for (EmployeeMTObis emp : depEmployees) {
            if (emp.getId().equals(emp1Id)) {
                empInDep--;
                Assert.assertEquals(emp1Id, emp.getId());
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            } else if (emp.getId().equals(emp2Id)) {
                empInDep--;
                Assert.assertEquals(emp2Id, emp.getId());
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
                Assert.assertEquals(depId, emp.getDepartment().getId());
                Assert.assertEquals("Computer Science", emp.getDepartment().getName());
            }
        }
    }
}
