package it.polimi.kundera.client.azuretable.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@NoArgsConstructor
@Entity
@Table(name = "PhoneEnum", schema = "azure-test@pu")
public class PhoneEnum {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "PHONE_ID")
    private String id;

    @Column(name = "NUMBER")
    private Long number;

    @Column(name = "TYPE_ENUM")
    @Enumerated(EnumType.STRING)
    private PhoneType type;
}
