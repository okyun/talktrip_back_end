package com.talktrip.talktrip.domain.product.repository;

import com.talktrip.talktrip.domain.product.dto.ProductWithAvgStarAndLike;
import org.springframework.data.domain.Slice;

import java.util.Optional;

public interface ProductSearchRepositoryCustom {

    Optional<ProductWithAvgStarAndLike> findByIdWithDetailsAndAvgStarAndLike(
            Long productId,
            Long memberId
    );

    Slice<ProductWithAvgStarAndLike> searchProductsWithAvgStarAndLikeCursor(
            String keyword,
            String countryName,
            Long memberId,
            java.time.LocalDateTime lastUpdatedAt,
            Long lastId,
            Double lastAvgStar,
            Integer lastMinPrice,
            int size,
            String sortKey
    );
}


