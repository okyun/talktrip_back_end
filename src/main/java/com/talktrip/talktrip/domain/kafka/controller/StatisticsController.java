package com.talktrip.talktrip.domain.kafka.controller;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderPurchaseStatResponse;
import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickStatResponse;
import com.talktrip.talktrip.domain.kafka.service.StatisticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 통계 조회 Controller
 * 
 * 상품 클릭 통계와 주문 구매 통계를 조회하는 API를 제공합니다.
 */
@RestController
@RequestMapping("/api/statistics")
@RequiredArgsConstructor
public class StatisticsController {

    private final StatisticsService statisticsService;

    /**
     * 상품 클릭 통계 TOP 30 조회
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds, 선택적)
     * @return 상품 클릭 통계 TOP 30
     */
    @GetMapping("/product-clicks/top30")
    public ResponseEntity<List<ProductClickStatResponse>> getTop30ProductClicks(
            @RequestParam(required = false) Long windowStartTime
    ) {
        List<ProductClickStatResponse> result = statisticsService.getTop30ProductClicks(windowStartTime);
        return ResponseEntity.ok(result);
    }

    /**
     * 주문 구매 통계 TOP 30 조회
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds, 선택적)
     * @return 주문 구매 통계 TOP 30
     */
    @GetMapping("/order-purchases/top30")
    public ResponseEntity<List<OrderPurchaseStatResponse>> getTop30OrderPurchases(
            @RequestParam(required = false) Long windowStartTime
    ) {
        List<OrderPurchaseStatResponse> result = statisticsService.getTop30OrderPurchases(windowStartTime);
        return ResponseEntity.ok(result);
    }
}

