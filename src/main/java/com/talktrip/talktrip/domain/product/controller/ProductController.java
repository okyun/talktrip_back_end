package com.talktrip.talktrip.domain.product.controller;

import com.talktrip.talktrip.domain.product.dto.response.ProductDetailResponse;
import com.talktrip.talktrip.domain.product.dto.response.ProductSummaryResponse;
import com.talktrip.talktrip.domain.product.service.ProductService;
import com.talktrip.talktrip.global.security.CustomMemberDetails;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

import static com.talktrip.talktrip.global.util.SortUtil.buildSort;

@Tag(name = "Product", description = "상품 관련 API")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/products")
public class ProductController {
    private final ProductService productService;

    @Operation(summary = "상품 목록 검색(커서 기반, 무한스크롤)")
    @GetMapping
    public ResponseEntity<com.talktrip.talktrip.domain.product.dto.response.ProductSliceResponse> getProducts(
            @RequestParam(required = false) String keyword,
            @RequestParam(defaultValue = "전체") String countryName,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(defaultValue = "updatedAt,desc") List<String> sort,
            @RequestParam(required = false) String cursor,
            @AuthenticationPrincipal CustomMemberDetails memberDetails
    ) {
        Long memberId = extractMemberId(memberDetails);
        String sortKey = (sort != null && !sort.isEmpty()) ? sort.get(0) : "updatedAt,desc";
        return ResponseEntity.ok(productService.searchProductsCursor(keyword, countryName, memberId, cursor, size, sortKey));
    }

    @Operation(summary = "상품 상세 조회")
    @GetMapping("/{productId}")
    public ResponseEntity<ProductDetailResponse> getProductDetail(
            @PathVariable Long productId,
            @AuthenticationPrincipal CustomMemberDetails memberDetails,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "3") int size,
            @RequestParam(defaultValue = "updatedAt,desc") List<String> sort
    ) {
        Pageable pageable = PageRequest.of(page, size, buildSort(sort));
        Long memberId = extractMemberId(memberDetails);
        return ResponseEntity.ok(productService.getProductDetail(productId, memberId, pageable));
    }

    private Long extractMemberId(CustomMemberDetails memberDetails) {
        return (memberDetails != null) ? memberDetails.getId() : null;
    }

}