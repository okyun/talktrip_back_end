package com.talktrip.talktrip.domain.product.entity;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;


/**
 * QProductStats is a Querydsl query type for ProductStats
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QProductStats extends EntityPathBase<ProductStats> {

    private static final long serialVersionUID = 1453944447L;

    public static final QProductStats productStats = new QProductStats("productStats");

    public final NumberPath<Double> avgStar = createNumber("avgStar", Double.class);

    public final NumberPath<Long> productId = createNumber("productId", Long.class);

    public final DateTimePath<java.time.LocalDateTime> updatedAt = createDateTime("updatedAt", java.time.LocalDateTime.class);

    public QProductStats(String variable) {
        super(ProductStats.class, forVariable(variable));
    }

    public QProductStats(Path<? extends ProductStats> path) {
        super(path.getType(), path.getMetadata());
    }

    public QProductStats(PathMetadata metadata) {
        super(ProductStats.class, metadata);
    }

}

