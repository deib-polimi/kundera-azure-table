package it.polimi.client.datastore.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
@Entity
@Table(name = "AddressCollection", schema = "gae-test@pu")
public class AddressCollection {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "ADDRESS_ID")
    private String id;

    @ElementCollection
    private List<String> streets;

    public void setStreets(String... streets) {
        this.streets = new ArrayList<String>();
        Collections.addAll(this.streets, streets);
    }
}
