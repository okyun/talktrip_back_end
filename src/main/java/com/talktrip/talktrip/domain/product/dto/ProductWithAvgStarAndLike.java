package com.talktrip.talktrip.domain.product.dto;

import com.talktrip.talktrip.domain.product.entity.Product;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ProductWithAvgStarAndLike {
    private Product product;
    private Double avgStar;     // 평균 별점 (캐시 또는 집계)
    private Boolean isLiked;    // 좋아요 여부
    private Integer minPrice;   // 미래 옵션 중 최저 할인가(캐시 또는 집계). 없으면 null
}
