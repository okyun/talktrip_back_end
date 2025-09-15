package com.talktrip.talktrip.global.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Table (indexes = {
    @Index(name = "idx_country_name", columnList = "name")
})
public class Country {

    @Id
    private Long id;

    private String continent;

    private String name;
}
