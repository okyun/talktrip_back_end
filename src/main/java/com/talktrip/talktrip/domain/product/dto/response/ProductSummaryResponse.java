package com.talktrip.talktrip.domain.product.dto.response;

import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.entity.ProductOption;
import java.time.LocalDateTime;
import lombok.Builder;
import lombok.Getter;

@Builder
public record ProductSummaryResponse(
        Long productId,
        String productName,
        String productDescription,
        String thumbnailImageUrl,
        int minPrice,
        int minDiscountPrice,
        Double averageReviewStar,
        boolean isLiked,
        LocalDateTime updatedAt
) {

    public static ProductSummaryResponse from(Product product, Double averageReviewStar, boolean isLiked) {
        ProductOption minPriceOption = product.getMinPriceOption();
        int minPrice = minPriceOption != null ? minPriceOption.getPrice() : 0;
        int minDiscountPrice = minPriceOption != null ? minPriceOption.getDiscountPrice() : 0;

        return ProductSummaryResponse.builder()
                .productId(product.getId())
                .productName(product.getProductName())
                .productDescription(product.getDescription())
                .thumbnailImageUrl(product.getThumbnailImageUrl())
                .minPrice(minPrice)
                .minDiscountPrice(minDiscountPrice)
                .averageReviewStar(averageReviewStar)
                .isLiked(isLiked)
                .updatedAt(product.getUpdatedAt())
                .build();
    }
}
