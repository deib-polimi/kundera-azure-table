package it.polimi.kundera.client.azuretable.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "PhoneInvalid2", schema = "azure-test@pu")
public class PhoneInvalid2 {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PHONE_ID")
    private Double id;

    @Column(name = "NUMBER")
    private Long number;
}
