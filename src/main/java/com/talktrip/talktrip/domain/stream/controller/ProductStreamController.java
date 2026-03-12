package com.talktrip.talktrip.domain.stream.controller;

import com.talktrip.talktrip.domain.member.entity.Member;
import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.domain.messaging.avro.KafkaEventProducer;
import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickStatResponse;
import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.repository.ProductRepository;
import com.talktrip.talktrip.domain.stream.service.ProductStreamsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 상품 관련 Kafka Streams Controller
 * 
 * 상품 클릭 통계 조회 및 상품 이벤트 발행 API를 제공합니다.
 * 
 * 주요 기능:
 * 1. 상품 클릭 통계 TOP 30 조회
 * 2. 상품 Streams 상태 확인
 * 3. 상품 클릭 이벤트 발행
 */
@Tag(name = "Product Streams", description = "상품 관련 Kafka Streams API")
@RestController
@RequestMapping("/api/streams/products")
@RequiredArgsConstructor
public class ProductStreamController {

    private static final Logger logger = LoggerFactory.getLogger(ProductStreamController.class);

    private final ProductStreamsService productStreamsService;
    private final KafkaEventProducer kafkaEventProducer;
    private final MemberRepository memberRepository;
    private final ProductRepository productRepository;

    /**
     * 상품 클릭 통계 TOP 30 조회
     * 
     * Kafka Streams State Store에서 상품 클릭 통계 TOP 30을 조회합니다.
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds, 선택적)
     *                        - null이면 모든 윈도우의 데이터를 조회하여 전체 TOP 30 반환
     *                        - 값이 있으면 해당 윈도우의 TOP 30 반환
     * @return 상품 클릭 통계 TOP 30 리스트
     */
    @Operation(
            summary = "상품 클릭 통계 TOP 30 조회",
            description = "Kafka Streams State Store에서 상품 클릭 통계 TOP 30을 조회합니다. " +
                         "windowStartTime 파라미터로 특정 윈도우의 통계를 조회하거나, " +
                         "파라미터를 생략하면 모든 윈도우의 데이터를 조회하여 전체 TOP 30을 반환합니다."
    )
    @GetMapping("/clicks/top30")
    public ResponseEntity<List<ProductClickStatResponse>> getTop30ProductClicks(
            @RequestParam(required = false) Long windowStartTime
    ) {
        List<ProductClickStatResponse> result = productStreamsService.getTop30ProductClicks(windowStartTime);
        return ResponseEntity.ok(result);
    }

    /**
     * 상품 Streams 상태 확인
     * 
     * @return 준비되었으면 true, 아니면 false
     */
    @Operation(
            summary = "상품 Streams 상태 확인",
            description = "상품 관련 Kafka Streams가 준비되었는지 확인합니다."
    )
    @GetMapping("/clicks/status")
    public ResponseEntity<Boolean> getProductStreamsStatus() {
        boolean isReady = productStreamsService.isReady();
        return ResponseEntity.ok(isReady);
    }

    /**
     * 상품 클릭 이벤트 발행
     * 
     * productId, memberId를 받아서 ProductEvent를 생성하고 발행합니다.
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID (선택적)
     * @return 이벤트 발행 성공 메시지
     */
    @Operation(
            summary = "상품 클릭 이벤트 발행",
            description = "productId, memberId를 받아서 상품 클릭 이벤트를 발행합니다."
    )
    @PostMapping("/click")
    public ResponseEntity<String> createProductClick(
            @RequestParam Long productId,
            @RequestParam(required = false) Long memberId
    ) {
        String userLabel = resolveUserLabel(memberId);
        String productLabel = resolveProductLabel(productId);
        logger.info("{} 사용자가 {} 상품을 클릭했습니다.", userLabel, productLabel);

        kafkaEventProducer.publishProductEvent(productId, memberId);
        return ResponseEntity.ok("Product click event published");
    }

    private String resolveUserLabel(Long memberId) {
        if (memberId == null) return "비회원";
        return memberRepository.findById(memberId)
                .map(m -> {
                    String n = m.getNickname();
                    if (n != null && !n.isBlank()) return n;
                    String name = m.getName();
                    return (name != null && !name.isBlank()) ? name : "회원(id:" + memberId + ")";
                })
                .orElse("회원(id:" + memberId + ")");
    }

    private String resolveProductLabel(Long productId) {
        if (productId == null) return "알 수 없음";
        return productRepository.findById(productId)
                .map(Product::getProductName)
                .orElse("상품(id:" + productId + ")");
    }

    // JSON 브랜치에서는 /click 하나의 엔드포인트만 유지하고,
    // 예전 Avro 테스트용 엔드포인트(/avro/publish)는 제거했습니다.
}


