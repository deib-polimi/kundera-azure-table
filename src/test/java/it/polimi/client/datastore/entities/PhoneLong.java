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
@Table(name = "PhoneLong", schema = "gae-test@pu")
public class PhoneLong {

    @Id
    @Column(name = "PHONE_ID")
    private Long id;

    @Column(name = "NUMBER")
    private Long number;
}
