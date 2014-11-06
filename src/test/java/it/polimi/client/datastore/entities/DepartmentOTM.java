package it.polimi.client.datastore.entities;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.*;
import java.util.List;

@Data
@ToString(exclude = "employees")
@EqualsAndHashCode(exclude = "employees")
@NoArgsConstructor
@Entity
@Table(name = "DepartmentOTM", schema = "gae-test@pu")
public class DepartmentOTM {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "DEPARTMENT_ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    /* a department employs many employees */
    @OneToMany(mappedBy = "department")
    private List<EmployeeMTObis> employees;
}
