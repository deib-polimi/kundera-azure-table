package it.polimi.client.datastore.entities;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@ToString(exclude = "projects")
@EqualsAndHashCode(exclude = "projects")
@NoArgsConstructor
@Entity
@Table(name = "EmployeeMTM", schema = "gae-test@pu")
public class EmployeeMTM {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "EMPLOYEE_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "SALARY")
    private Long salary;

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinTable(name = "EMPLOYEE_PROJECT",
            joinColumns = {@JoinColumn(name = "EMPLOYEE_ID")},
            inverseJoinColumns = {@JoinColumn(name = "PROJECT_ID")})
    private List<ProjectMTM> projects;

    public void addProjects(ProjectMTM... projects) {
        if (this.projects == null) {
            this.projects = new ArrayList<ProjectMTM>();
        }
        Collections.addAll(this.projects, projects);
    }
}
