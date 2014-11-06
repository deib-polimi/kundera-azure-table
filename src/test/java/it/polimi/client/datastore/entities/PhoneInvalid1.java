package it.polimi.client.datastore.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@NoArgsConstructor
@Entity
@Table(name = "PhoneInvalid1", schema = "gae-test@pu")
public class PhoneInvalid1 {

    @Id
    @Column(name = "PHONE_ID")
    private Double id;

    @Column(name = "NUMBER")
    private Long number;
}
