package com.talktrip.talktrip.domain.product.service;

import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.domain.product.dto.ProductWithAvgStarAndLike;
import com.talktrip.talktrip.domain.product.dto.response.ProductDetailResponse;
import com.talktrip.talktrip.domain.product.dto.response.ProductSliceResponse;
import com.talktrip.talktrip.domain.product.dto.response.ProductSummaryResponse;
import com.talktrip.talktrip.domain.product.repository.ProductRepository;
import com.talktrip.talktrip.domain.review.dto.response.ReviewResponse;
import com.talktrip.talktrip.domain.review.entity.Review;
import com.talktrip.talktrip.domain.review.repository.ReviewRepository;
import com.talktrip.talktrip.global.exception.ErrorCode;
import com.talktrip.talktrip.global.exception.MemberException;
import com.talktrip.talktrip.global.exception.ProductException;
import com.talktrip.talktrip.global.repository.CountryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductService {

    private static final String ALL_COUNTRIES = "전체";

    private final ProductRepository productRepository;
    private final ReviewRepository reviewRepository;
    private final MemberRepository memberRepository;
    private final CountryRepository countryRepository;

    @Transactional(propagation = Propagation.NOT_SUPPORTED, readOnly = true)
    public ProductSliceResponse searchProductsCursor(
            String keyword, String countryName, Long memberId, String cursor, int size, String sortKey
    ) {
        validateMember(memberId);
        validateCountry(countryName);

        java.time.LocalDateTime lastUpdatedAt = null;
        Long lastId = null;
        Double lastAvgStar = null;
        Integer lastMinPrice = null;

        if (cursor != null && !cursor.isBlank()) {
            try {
                String json = new String(java.util.Base64.getUrlDecoder().decode(cursor));
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                var node = mapper.readTree(json);
                if (node.hasNonNull("updatedAt")) lastUpdatedAt = java.time.LocalDateTime.parse(node.get("updatedAt").asText());
                if (node.hasNonNull("id")) lastId = node.get("id").asLong();
                if (node.hasNonNull("avgStar")) lastAvgStar = node.get("avgStar").asDouble();
                if (node.has("minPrice") && !node.get("minPrice").isNull()) lastMinPrice = node.get("minPrice").asInt();
            } catch (Exception ignore) {}
        }

        var slice = productRepository.searchProductsWithAvgStarAndLikeCursor(
                keyword, countryName, memberId, lastUpdatedAt, lastId, lastAvgStar, lastMinPrice, size, sortKey
        );

        List<ProductSummaryResponse> items = slice.getContent().stream()
                .map(r -> ProductSummaryResponse.from(r.getProduct(), r.getAvgStar(), r.getIsLiked()))
                .toList();

        java.time.LocalDateTime nextUpdated = null;
        Long nextId = null;
        Double nextAvg = null;
        Integer nextMin = null;

        if (!slice.getContent().isEmpty()) {
            var last = slice.getContent().get(slice.getContent().size() - 1);
            nextUpdated = last.getProduct().getUpdatedAt();
            nextId = last.getProduct().getId();
            nextAvg = last.getAvgStar();
            nextMin = last.getMinPrice();
        }

        String nextCursor = null;
        if (slice.hasNext() && nextId != null) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                var node = mapper.createObjectNode();
                node.put("updatedAt", nextUpdated != null ? nextUpdated.toString() : null);
                node.put("id", nextId);
                String key = (sortKey == null ? "updatedAt" : sortKey.split(",")[0]);
                if ("rating".equals(key)) node.put("avgStar", nextAvg);
                if ("minPrice".equals(key)) node.put("minPrice", nextMin);
                nextCursor = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(mapper.writeValueAsBytes(node));
            } catch (Exception ignore) {}
        }

        return new ProductSliceResponse(items, slice.hasNext(), nextCursor);
    }

    @Transactional(readOnly = true)
    public ProductDetailResponse getProductDetail(Long productId, Long memberId, Pageable pageable) {
        validateMember(memberId);
        validateProduct(productId);

        ProductWithAvgStarAndLike dto = productRepository.findByIdWithDetailsAndAvgStarAndLike(productId, memberId)
                .orElseThrow(() -> new ProductException(ErrorCode.PRODUCT_NOT_FOUND));

        Page<Review> reviewPage = reviewRepository.findByProductIdWithPaging(productId, pageable);
        List<ReviewResponse> reviewResponses = ReviewResponse.to(reviewPage.getContent(), dto.getProduct());

        return ProductDetailResponse.from(dto.getProduct(), dto.getAvgStar(), reviewResponses, dto.getIsLiked());
    }

    private void validateMember(Long memberId) {
        if (memberId != null && !memberRepository.existsById(memberId)) throw new MemberException(ErrorCode.USER_NOT_FOUND);
    }
    private void validateProduct(Long productId) {
        if (productId == null || !productRepository.existsById(productId)) throw new ProductException(ErrorCode.PRODUCT_NOT_FOUND);
    }
    private void validateCountry(String countryName) {
        if (countryName == null || countryName.isBlank() || ALL_COUNTRIES.equals(countryName)) return;
        if (!countryRepository.existsByName(countryName.trim())) throw new ProductException(ErrorCode.COUNTRY_NOT_FOUND);
    }
}