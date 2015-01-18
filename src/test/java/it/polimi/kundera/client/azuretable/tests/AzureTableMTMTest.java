package it.polimi.kundera.client.azuretable.tests;

import it.polimi.kundera.client.azuretable.entities.EmployeeMTM;
import it.polimi.kundera.client.azuretable.entities.ProjectMTM;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.TypedQuery;
import java.util.List;

/**
 * @author Fabio Arcidiacono.
 */
public class AzureTableMTMTest extends TestBase {

    @Test
    public void testCRUD() {
        print("create");
        ProjectMTM project1 = new ProjectMTM();
        project1.setName("Project 1");

        ProjectMTM project2 = new ProjectMTM();
        project2.setName("Project 2");

        ProjectMTM project3 = new ProjectMTM();
        project3.setName("Project 3");

        EmployeeMTM employee1 = new EmployeeMTM();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.addProjects(project1, project2);
        em.persist(employee1);

        EmployeeMTM employee2 = new EmployeeMTM();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        employee2.addProjects(project2, project3);
        em.persist(employee2);

        String prj1Id = project1.getId();
        String prj2Id = project2.getId();
        String prj3Id = project3.getId();
        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("read");
        print("employee 1");
        EmployeeMTM foundEmployee1 = em.find(EmployeeMTM.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Fabio", foundEmployee1.getName());
        Assert.assertEquals((Long) 123L, foundEmployee1.getSalary());
        print("access projects");
        int projectCount = 2;
        int project1Employees = 1;
        int project2Employees = 2;
        for (ProjectMTM project : foundEmployee1.getProjects()) {
            if (project.getId().equals(prj1Id)) {
                projectCount--;
                Assert.assertEquals(prj1Id, project.getId());
                Assert.assertEquals("Project 1", project.getName());
                print("access employees project 1");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp1Id)) {
                        project1Employees--;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    }
                }
            } else if (project.getId().equals(prj2Id)) {
                projectCount--;
                Assert.assertEquals(prj2Id, project.getId());
                Assert.assertEquals("Project 2", project.getName());
                print("access employees project 2");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(0, projectCount);
        Assert.assertEquals(0, project1Employees);
        Assert.assertEquals(0, project2Employees);

        print("employee 2");
        EmployeeMTM foundEmployee2 = em.find(EmployeeMTM.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        print("access projects");
        projectCount = 2;
        project2Employees = 2;
        int project3Employees = 1;
        for (ProjectMTM project : foundEmployee2.getProjects()) {
            if (project.getId().equals(prj2Id)) {
                projectCount--;
                Assert.assertEquals(prj2Id, project.getId());
                Assert.assertEquals("Project 2", project.getName());
                print("access employees project 2");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            } else if (project.getId().equals(prj3Id)) {
                projectCount--;
                Assert.assertEquals(prj3Id, project.getId());
                Assert.assertEquals("Project 3", project.getName());
                print("access employees project 3");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp2Id)) {
                        project3Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(0, projectCount);
        Assert.assertEquals(0, project2Employees);
        Assert.assertEquals(0, project3Employees);

        print("update");
        project1.setName("Project 11");
        project2.setName("Project 22");
        project3.setName("Project 33");
        em.merge(project1);
        em.merge(project2);
        em.merge(project3);

        clear();

        print("employee 1");
        foundEmployee1 = em.find(EmployeeMTM.class, emp1Id);
        Assert.assertNotNull(foundEmployee1);
        Assert.assertEquals(emp1Id, foundEmployee1.getId());
        Assert.assertEquals("Fabio", foundEmployee1.getName());
        Assert.assertEquals((Long) 123L, foundEmployee1.getSalary());
        print("access projects");
        projectCount = 2;
        project1Employees = 1;
        project2Employees = 2;
        for (ProjectMTM project : foundEmployee1.getProjects()) {
            if (project.getId().equals(prj1Id)) {
                projectCount--;
                Assert.assertEquals(prj1Id, project.getId());
                Assert.assertEquals("Project 11", project.getName());
                print("access employees project 11");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp1Id)) {
                        project1Employees--;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    }
                }
            } else if (project.getId().equals(prj2Id)) {
                projectCount--;
                Assert.assertEquals(prj2Id, project.getId());
                Assert.assertEquals("Project 22", project.getName());
                print("access employees project 22");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(0, projectCount);
        Assert.assertEquals(0, project1Employees);
        Assert.assertEquals(0, project2Employees);

        print("employee 2");
        foundEmployee2 = em.find(EmployeeMTM.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        print("access projects");
        projectCount = 2;
        project2Employees = 2;
        project3Employees = 1;
        for (ProjectMTM project : foundEmployee2.getProjects()) {
            if (project.getId().equals(prj2Id)) {
                projectCount--;
                Assert.assertEquals(prj2Id, project.getId());
                Assert.assertEquals("Project 22", project.getName());
                print("access employees project 22");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp1Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp1Id, emp.getId());
                        Assert.assertEquals("Fabio", emp.getName());
                        Assert.assertEquals((Long) 123L, emp.getSalary());
                    } else if (emp.getId().equals(emp2Id)) {
                        project2Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            } else if (project.getId().equals(prj3Id)) {
                projectCount--;
                Assert.assertEquals(prj3Id, project.getId());
                Assert.assertEquals("Project 33", project.getName());
                print("access employees project 33");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp2Id)) {
                        project3Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(0, projectCount);
        Assert.assertEquals(0, project2Employees);
        Assert.assertEquals(0, project3Employees);

        print("delete");
        em.remove(foundEmployee1);
        foundEmployee1 = em.find(EmployeeMTM.class, emp1Id);
        Assert.assertNull(foundEmployee1);
        project1 = em.find(ProjectMTM.class, prj1Id);
        Assert.assertNull(project1);
        project2 = em.find(ProjectMTM.class, prj2Id);
        Assert.assertNull(project2);

        project3 = em.find(ProjectMTM.class, prj3Id);
        Assert.assertNotNull(project3);

        print("employee 2");
        foundEmployee2 = em.find(EmployeeMTM.class, emp2Id);
        Assert.assertNotNull(foundEmployee2);
        Assert.assertEquals(emp2Id, foundEmployee2.getId());
        Assert.assertEquals("Crizia", foundEmployee2.getName());
        Assert.assertEquals((Long) 456L, foundEmployee2.getSalary());
        print("access projects");
        projectCount = 1;
        project3Employees = 1;
        for (ProjectMTM project : foundEmployee2.getProjects()) {
            if (project == null) {
                print("detected null project");
            }
            if (project != null && project.getId().equals(prj3Id)) {
                projectCount--;
                Assert.assertEquals(prj3Id, project.getId());
                Assert.assertEquals("Project 33", project.getName());
                print("access employees project 33");
                for (EmployeeMTM emp : project.getEmployees()) {
                    if (emp.getId().equals(emp2Id)) {
                        project3Employees--;
                        Assert.assertEquals(emp2Id, emp.getId());
                        Assert.assertEquals("Crizia", emp.getName());
                        Assert.assertEquals((Long) 456L, emp.getSalary());
                    }
                }
            }
        }
        Assert.assertEquals(0, projectCount);
        Assert.assertEquals(0, project3Employees);
    }

    @Test
    public void testQuery() {
        print("create");
        ProjectMTM project1 = new ProjectMTM();
        project1.setName("Project 1");

        ProjectMTM project2 = new ProjectMTM();
        project2.setName("Project 2");

        ProjectMTM project3 = new ProjectMTM();
        project3.setName("Project 3");

        EmployeeMTM employee1 = new EmployeeMTM();
        employee1.setName("Fabio");
        employee1.setSalary(123L);
        employee1.addProjects(project1, project2);
        em.persist(employee1);

        EmployeeMTM employee2 = new EmployeeMTM();
        employee2.setName("Crizia");
        employee2.setSalary(456L);
        employee2.addProjects(project2, project3);
        em.persist(employee2);

        String prj1Id = project1.getId();
        String prj2Id = project2.getId();
        String prj3Id = project3.getId();
        String emp1Id = employee1.getId();
        String emp2Id = employee2.getId();
        clear();

        print("select all");
        TypedQuery<EmployeeMTM> query = em.createQuery("SELECT e FROM EmployeeMTM e", EmployeeMTM.class);
        List<EmployeeMTM> allEmployees = query.getResultList();
        int empToCheck = 2;
        int emp1projectsToCheck = 2;
        int emp2projectsToCheck = 2;
        for (EmployeeMTM emp : allEmployees) {
            Assert.assertNotNull(emp.getId());
            if (emp.getId().equals(emp1Id)) {
                empToCheck--;
                Assert.assertEquals("Fabio", emp.getName());
                Assert.assertEquals((Long) 123L, emp.getSalary());
                for (ProjectMTM project : emp.getProjects()) {
                    if (project.getId().equals(prj1Id)) {
                        emp1projectsToCheck--;
                        Assert.assertEquals(prj1Id, project.getId());
                        Assert.assertEquals("Project 1", project.getName());
                    } else if (project.getId().equals(prj2Id)) {
                        emp1projectsToCheck--;
                        Assert.assertEquals(prj2Id, project.getId());
                        Assert.assertEquals("Project 2", project.getName());
                    }
                }
            } else if (emp.getId().equals(emp2Id)) {
                empToCheck--;
                Assert.assertEquals("Crizia", emp.getName());
                Assert.assertEquals((Long) 456L, emp.getSalary());
                for (ProjectMTM project : emp.getProjects()) {
                    if (project.getId().equals(prj2Id)) {
                        emp2projectsToCheck--;
                        Assert.assertEquals(prj2Id, project.getId());
                        Assert.assertEquals("Project 2", project.getName());
                    } else if (project.getId().equals(prj3Id)) {
                        emp2projectsToCheck--;
                        Assert.assertEquals(prj3Id, project.getId());
                        Assert.assertEquals("Project 3", project.getName());
                    }
                }
            }
        }
        Assert.assertEquals(0, empToCheck);
        Assert.assertEquals(0, emp1projectsToCheck);
        Assert.assertEquals(0, emp2projectsToCheck);

        /*
         * NOTE: cannot directly query over join table
         * EMPLOYEE_PROJECT since is not a class in JPA model
         */
    }
}
