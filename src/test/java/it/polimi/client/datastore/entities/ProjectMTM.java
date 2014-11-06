package it.polimi.client.datastore.entities;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.*;
import java.util.Collections;
import java.util.List;

@Data
@ToString(exclude = "employees")
@EqualsAndHashCode(exclude = "employees")
@NoArgsConstructor
@Entity
@Table(name = "ProjectMTM", schema = "gae-test@pu")
public class ProjectMTM {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PROJECT_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @ManyToMany(mappedBy = "projects")
    private List<EmployeeMTM> employees;

    public void addEmployees(EmployeeMTM... employees) {
        Collections.addAll(this.employees, employees);
    }
}
