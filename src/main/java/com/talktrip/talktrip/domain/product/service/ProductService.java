package com.talktrip.talktrip.domain.product.service;

import com.talktrip.talktrip.domain.like.repository.LikeRepository;
import com.talktrip.talktrip.domain.product.dto.response.ProductDetailResponse;
import com.talktrip.talktrip.domain.product.dto.response.ProductSearchPageCache;
import com.talktrip.talktrip.domain.product.dto.response.ProductSummaryResponse;
import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.entity.ProductOption;
import com.talktrip.talktrip.domain.product.repository.ProductRepository;
import com.talktrip.talktrip.domain.review.dto.response.ReviewResponse;
import com.talktrip.talktrip.domain.review.entity.Review;
import com.talktrip.talktrip.domain.review.repository.ReviewRepository;
import com.talktrip.talktrip.global.exception.ErrorCode;
import com.talktrip.talktrip.global.exception.ProductException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.util.*;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final ReviewRepository reviewRepository;
    private final LikeRepository likeRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    @Lazy
    @Autowired
    private ProductService self;

    @Value("${fastapi.base-url}")
    private String fastApiBaseUrl;

    @Transactional(readOnly = true)
    public Page<ProductSummaryResponse> searchProducts(
            String keyword,
            String countryName,
            Long memberId,
            Pageable pageable
    ) {
        return self.searchProductsCached(keyword, countryName, memberId, pageable).toPage(pageable);
    }

    /**
     * 상품 목록(검색·국가·정렬·페이지) Redis 캐시 — 키에 memberId 포함(좋아요 여부 반영).
     */
    @Cacheable(
            cacheNames = "product",
            key = "'search:' + (#keyword == null ? \"\" : #keyword.trim()) + ':' + #countryName + ':m:' + (#memberId != null ? #memberId : -1L) + ':p:' + #pageable.pageNumber + ':s:' + #pageable.pageSize + ':sort:' + #pageable.sort.toString()"
    )
    @Transactional(readOnly = true)
    public ProductSearchPageCache searchProductsCached(
            String keyword,
            String countryName,
            Long memberId,
            Pageable pageable
    ) {
        Page<Product> page = (keyword == null || keyword.isBlank())
                ? productRepository.findVisibleProducts(countryName, pageable)
                : productRepository.searchByKeywords(
                Arrays.stream(keyword.trim().split("\\s+"))
                        .filter(s -> !s.isBlank()).toList(),
                countryName,
                pageable
        );

        List<Product> products = page.getContent();
        if (products.isEmpty()) {
            return new ProductSearchPageCache(List.of(), page.getTotalElements());
        }

        List<Long> productIds = products.stream().map(Product::getId).toList();

        Map<Long, Double> avgStarMap = reviewRepository.fetchAvgStarsByProductIds(productIds);

        Set<Long> likedProductIds = (memberId == null)
                ? Set.of()
                : likeRepository.findLikedProductIds(memberId, productIds);

        List<ProductSummaryResponse> content = products.stream().map(p -> {
            float avgStar = avgStarMap.getOrDefault(p.getId(), 0.0).floatValue();
            boolean liked = likedProductIds.contains(p.getId());
            return ProductSummaryResponse.from(p, avgStar, liked);
        }).toList();

        return new ProductSearchPageCache(content, page.getTotalElements());
    }

    @Transactional(readOnly = true)
    public ProductDetailResponse getProductDetail(
            Long productId,
            Long memberId,
            Pageable pageable
    ) {
        return self.getProductDetailCached(productId, memberId, pageable);
    }

    /**
     * 상품 상세(리뷰 페이지·좋아요·옵션 요약) Redis 캐시.
     */
    @Cacheable(
            cacheNames = "product",
            key = "'detail:' + #productId + ':m:' + (#memberId != null ? #memberId : -1L) + ':p:' + #pageable.pageNumber + ':s:' + #pageable.pageSize + ':sort:' + #pageable.sort.toString()"
    )
    @Transactional(readOnly = true)
    public ProductDetailResponse getProductDetailCached(
            Long productId,
            Long memberId,
            Pageable pageable
    ) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductException(ErrorCode.PRODUCT_NOT_FOUND));

        int futureStock = product.getProductOptions().stream()
                //.filter(option -> !option.getStartDate().isBefore(LocalDate.now()))
                .mapToInt(ProductOption::getStock)
                .sum();

        if (futureStock == 0) {
            throw new ProductException(ErrorCode.PRODUCT_NOT_FOUND);
        }

        Page<Review> reviewPage = reviewRepository.findByProductId(productId, pageable);

        float avgStar = (float) reviewRepository.findByProductId(productId).stream()
                .mapToDouble(Review::getReviewStar)
                .average()
                .orElse(0.0);

        List<ReviewResponse> reviewResponses = reviewPage.stream()
                .map(review -> ReviewResponse.from(review, product))
                .toList();


        boolean isLiked = (memberId != null) &&
                likeRepository.existsByProductIdAndMemberId(productId, memberId);

        return ProductDetailResponse.from(product, avgStar, reviewResponses, isLiked);
    }

    @Transactional(readOnly = true)
    public List<ProductSummaryResponse> aiSearchProducts(
            String query,
            Long memberId
    ) {
        try {
            String fastApiUrl = fastApiBaseUrl + "/query";

            Map<String, String> requestBody = Map.of("query", query);

            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.postForObject(
                    fastApiUrl,
                    requestBody,
                    Map.class
            );

            if (response == null || !response.containsKey("product_ids")) {
                return List.of();
            }

            Object productIdsObj = response.get("product_ids");
            List<String> productIdStrings;

            if (productIdsObj instanceof List<?>) {
                productIdStrings = ((List<?>) productIdsObj).stream()
                        .map(Object::toString)
                        .toList();
            } else {
                return List.of();
            }

            if (productIdStrings.isEmpty()) {
                return List.of();
            }

            List<Long> productIds = productIdStrings.stream()
                    .map(Long::parseLong)
                    .toList();

            List<Product> products = productRepository.findAllById(productIds);

            Map<Long, Integer> idOrder = new HashMap<>();
            for (int i = 0; i < productIds.size(); i++) {
                idOrder.put(productIds.get(i), i);
            }

            return products.stream()
                    .sorted(Comparator.comparing(p -> idOrder.getOrDefault(p.getId(), Integer.MAX_VALUE)))
                    .map(product -> {
                        List<Review> allReviews = reviewRepository.findByProductId(product.getId());
                        float avgStar = (float) allReviews.stream()
                                .mapToDouble(Review::getReviewStar)
                                .average()
                                .orElse(0.0);

                        boolean liked = (memberId != null) &&
                                likeRepository.existsByProductIdAndMemberId(product.getId(), memberId);

                        return ProductSummaryResponse.from(product, avgStar, liked);
                    })
                    .toList();

        } catch (Exception e) {
            throw new ProductException(ErrorCode.PRODUCT_NOT_FOUND);
        }
    }
}
