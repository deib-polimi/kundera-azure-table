package it.polimi.kundera.client.azuretable.tests;

import com.impetus.kundera.KunderaException;
import it.polimi.kundera.client.azuretable.entities.Employee;
import it.polimi.kundera.client.azuretable.entities.PhoneEnum;
import it.polimi.kundera.client.azuretable.entities.PhoneType;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.TypedQuery;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTableQueryTest extends TestBase {

    @Test
    public void testSelectQuery() {
        print("create");
        Employee employee1 = new Employee();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        em.persist(employee1);

        Employee employee2 = new Employee();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        em.persist(employee2);

        PhoneEnum phone = new PhoneEnum();
        phone.setNumber(123L);
        phone.setType(PhoneType.HOME);
        em.persist(phone);

        String phnId = phone.getId();
        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("select all");
        TypedQuery<Employee> query = em.createQuery("SELECT e FROM Employee e", Employee.class);
        List<Employee> allEmployees = query.getResultList();
        Assert.assertNotNull(allEmployees);
        Assert.assertEquals(2, allEmployees.size());
        int toCheck = 2;
        for (Employee emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, toCheck);

        clear();

        print("select property");
        query = em.createQuery("SELECT e.name FROM Employee e WHERE e.id = :id", Employee.class);
        Employee foundEmployee = query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertNull(foundEmployee.getSalary());

        clear();

        print("where clause");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.id = :id", Employee.class);
        foundEmployee = query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp1Id, foundEmployee.getId());
        Assert.assertEquals("Fabio", foundEmployee.getName());
        Assert.assertEquals((Long) 123L, foundEmployee.getSalary());

        clear();

        print("complex where clause");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.name = :n AND e.salary = :s", Employee.class);
        foundEmployee = query.setParameter("n", "Crizia").setParameter("s", 456L).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp2Id, foundEmployee.getId());
        Assert.assertEquals("Crizia", foundEmployee.getName());
        Assert.assertEquals((Long) 456L, foundEmployee.getSalary());

        clear();

        print("where over enumerated");
        TypedQuery<PhoneEnum> enumQuery = em.createQuery("SELECT p FROM PhoneEnum p WHERE p.type = :type", PhoneEnum.class);
        PhoneEnum foundPhone = enumQuery.setParameter("type", PhoneType.HOME.toString()).getSingleResult();
        Assert.assertNotNull(foundPhone);
        Assert.assertEquals(phnId, foundPhone.getId());
        Assert.assertEquals((Long) 123L, foundPhone.getNumber());
        Assert.assertEquals(PhoneType.HOME, foundPhone.getType());
    }

    @Test
    public void testComparisonOperators() {
        print("create");
        Employee employee1 = new Employee();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        em.persist(employee1);

        Employee employee2 = new Employee();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        em.persist(employee2);

        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("greater than");
        TypedQuery<Employee> query = em.createQuery("SELECT e FROM Employee e WHERE e.salary > :s", Employee.class);
        List<Employee> foundEmployees = query.setParameter("s", 123L).getResultList();
        Assert.assertNotNull(foundEmployees);
        Assert.assertEquals(1, foundEmployees.size());
        Assert.assertNotNull(foundEmployees.get(0));
        Assert.assertEquals(emp2Id, foundEmployees.get(0).getId());
        Assert.assertEquals("Crizia", foundEmployees.get(0).getName());
        Assert.assertEquals((Long) 456L, foundEmployees.get(0).getSalary());

        print("greater than or equal");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.salary >= :s", Employee.class);
        foundEmployees = query.setParameter("s", 123L).getResultList();
        Assert.assertNotNull(foundEmployees);
        Assert.assertEquals(2, foundEmployees.size());
        int toCheck = 2;
        for (Employee emp : foundEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, toCheck);

        print("less than");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.salary < :s", Employee.class);
        foundEmployees = query.setParameter("s", 456L).getResultList();
        Assert.assertNotNull(foundEmployees);
        Assert.assertEquals(1, foundEmployees.size());
        Assert.assertNotNull(foundEmployees.get(0));
        Assert.assertEquals(emp1Id, foundEmployees.get(0).getId());
        Assert.assertEquals("Fabio", foundEmployees.get(0).getName());
        Assert.assertEquals((Long) 123L, foundEmployees.get(0).getSalary());

        print("less than or equal");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.salary <= :s", Employee.class);
        foundEmployees = query.setParameter("s", 456L).getResultList();
        Assert.assertNotNull(foundEmployees);
        Assert.assertEquals(2, foundEmployees.size());
        toCheck = 2;
        for (Employee emp : foundEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, toCheck);
    }

    @Test
    public void testOperators() {
        print("create");
        Employee employee1 = new Employee();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        em.persist(employee1);

        Employee employee2 = new Employee();
        employee2.setName("Crizia");
        employee2.setSalary(345L);
        em.persist(employee2);

        Employee employee3 = new Employee();
        employee3.setName("Giuseppe");
        employee3.setSalary(567L);
        em.persist(employee3);

        Employee employee4 = new Employee();
        employee4.setName("Cinzia");
        employee4.setSalary(789L);
        em.persist(employee4);

        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        String emp3Id = employee3.getId();
        clear();

        print("limit");
        TypedQuery<Employee> query = em.createQuery("SELECT e FROM Employee e", Employee.class);
        List<Employee> foundEmployees = query.setMaxResults(2).getResultList();
        Assert.assertNotNull(foundEmployees);
        Assert.assertEquals(2, foundEmployees.size());
        for (Employee emp : foundEmployees) {
            System.out.println(emp);
        }

        print("between");
        query = em.createQuery("SELECT e FROM Employee e WHERE e.salary BETWEEN :start AND :end", Employee.class);
        foundEmployees = query.setParameter("start", 123L).setParameter("end", 567L).getResultList();
        Assert.assertNotNull(foundEmployees);
        Assert.assertEquals(3, foundEmployees.size());
        int toCheck = 3;
        for (Employee emp : foundEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                toCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
            } else if (emp.getId().equals(emp2Id)) {
                toCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 345L, emp.getSalary());
            } else if (emp.getId().equals(emp3Id)) {
                toCheck--;
                Assert.assertEquals("Giuseppe", emp.getName());
                Assert.assertEquals((Long) 567L, emp.getSalary());
            }
        }
        Assert.assertEquals(0, toCheck);

        /* IN operator is not supported */
        query = em.createQuery("SELECT e FROM Employee e WHERE e.name IN ('Fabio', 'Crizia')", Employee.class);
        thrown.expect(KunderaException.class);
        query.getResultList();

        /* LIKE operator is not supported */
        query = em.createQuery("SELECT e FROM Employee e WHERE e.name LIKE :name", Employee.class);
        query.setParameter("name", "Fabio");
        thrown.expect(KunderaException.class);
        query.getResultList();
    }

    @Test
    public void testUpdateDeleteQuery() {
        print("create");
        Employee employee = new Employee();
        employee.setName("Fabio");
        employee.setSalary(123L);
        em.persist(employee);

        String emp1Id = employee.getId();
        clear();

        print("update");
        TypedQuery<Employee> query = em.createQuery("UPDATE Employee SET salary = :s, name =:n2 WHERE name = :n", Employee.class);
        int updated = query.setParameter("s", 789L).setParameter("n2", "Pippo").setParameter("n", "Fabio").executeUpdate();
        Assert.assertEquals(1, updated);

        query = em.createQuery("SELECT e FROM Employee e WHERE e.id = :id", Employee.class);
        Employee foundEmployee = query.setParameter("id", emp1Id).getSingleResult();
        Assert.assertNotNull(foundEmployee);
        Assert.assertEquals(emp1Id, foundEmployee.getId());
        Assert.assertEquals("Pippo", foundEmployee.getName());
        Assert.assertEquals((Long) 789L, foundEmployee.getSalary());

        print("delete");
        query = em.createQuery("DELETE FROM Employee e WHERE e.name = :n", Employee.class);
        int deleted = query.setParameter("n", "Pippo").executeUpdate();
        Assert.assertEquals(1, deleted);

        query = em.createQuery("SELECT e FROM Employee e", Employee.class);
        List<Employee> allEmployees = query.getResultList();
        Assert.assertNotNull(allEmployees);
        Assert.assertTrue(allEmployees.isEmpty());
    }
}
