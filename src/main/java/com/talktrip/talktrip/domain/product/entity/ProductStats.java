package com.talktrip.talktrip.domain.product.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "product_stats")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductStats {

    @Id
    @Column(name = "product_id")
    private Long productId;

    @Column(name = "avg_star")
    private Double avgStar;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    public static ProductStats of(Long productId, Double avgStar) {
        return ProductStats.builder()
                .productId(productId)
                .avgStar(avgStar)
                .updatedAt(LocalDateTime.now())
                .build();
    }
}


