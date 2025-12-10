package com.talktrip.talktrip.domain.product.service;

import com.talktrip.talktrip.domain.product.entity.ProductOption;
import com.talktrip.talktrip.domain.product.repository.ProductOptionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 관리 서비스
 * 재고 확인, 차감, 복원 및 변경 이력을 추적하고 관리
 */
@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class StockService {

    private final ProductOptionRepository productOptionRepository;

    /**
     * 재고 확인 및 차감 (주문 생성 시 사용)
     * @param productOptionId 상품 옵션 ID
     * @param quantity 차감할 수량
     * @return 재고 차감된 ProductOption
     * @throws IllegalStateException 재고가 부족한 경우
     * @throws IllegalArgumentException 옵션을 찾을 수 없는 경우
     */
    public ProductOption checkStockAndDecrease(Long productOptionId, int quantity) {
        ProductOption productOption = productOptionRepository.findById(productOptionId)
                .orElseThrow(() -> new IllegalArgumentException("옵션을 찾을 수 없습니다: " + productOptionId));

        // 재고 확인
        if (productOption.getStock() < quantity) {
            throw new IllegalStateException("재고 부족: " + productOption.getOptionName() + 
                    " (현재 재고: " + productOption.getStock() + ", 요청 수량: " + quantity + ")");
        }

        // 재고 차감
        int beforeStock = productOption.getStock();
        productOption.setStock(beforeStock - quantity);
        
        // 재고 변경 이력 기록
        recordStockChange(productOptionId, -quantity, "ORDER_CREATED");
        
        log.info("재고 차감 완료: productOptionId={}, optionName={}, beforeStock={}, quantity={}, afterStock={}", 
                productOptionId, productOption.getOptionName(), beforeStock, quantity, productOption.getStock());
        
        return productOption;
    }

    /**
     * 재고 복원 (주문 취소 시 사용)
     * @param productOptionId 상품 옵션 ID
     * @param quantity 복원할 수량
     */
    public void restoreStock(Long productOptionId, int quantity) {
        ProductOption productOption = productOptionRepository.findById(productOptionId)
                .orElse(null);
        
        if (productOption == null) {
            log.warn("재고 복원 실패: 옵션을 찾을 수 없습니다. productOptionId={}, quantity={}", 
                    productOptionId, quantity);
            return;
        }

        int beforeStock = productOption.getStock();
        productOption.addStock(quantity);
        
        // 재고 변경 이력 기록
        recordStockChange(productOptionId, quantity, "ORDER_CANCELLED");
        
        log.info("재고 복원 완료: productOptionId={}, optionName={}, beforeStock={}, quantity={}, afterStock={}", 
                productOptionId, productOption.getOptionName(), beforeStock, quantity, productOption.getStock());
    }

    /**
     * 재고 확인만 수행 (재고 차감 없이 확인만)
     * @param productOptionId 상품 옵션 ID
     * @param quantity 확인할 수량
     * @return 재고가 충분한지 여부
     * @throws IllegalArgumentException 옵션을 찾을 수 없는 경우
     */
    public boolean checkStock(Long productOptionId, int quantity) {
        ProductOption productOption = productOptionRepository.findById(productOptionId)
                .orElseThrow(() -> new IllegalArgumentException("옵션을 찾을 수 없습니다: " + productOptionId));
        
        return productOption.getStock() >= quantity;
    }

    /**
     * 재고 변경 이력 기록
     * @param productOptionId 상품 옵션 ID
     * @param quantity 변경 수량 (음수: 차감, 양수: 증가)
     * @param reason 변경 사유 (ORDER_CREATED, ORDER_CANCELLED 등)
     */
    private void recordStockChange(Long productOptionId, int quantity, String reason) {
        try {
            // 실제 구현에서는 재고 변경 이력 테이블에 기록
            // 여기서는 로그만 남김
            log.info("재고 변경 이력: productOptionId={}, quantity={}, reason={}", 
                    productOptionId, quantity, reason);
            
            // 필요 시 재고 변경 이력 엔티티에 저장
            // StockHistory stockHistory = StockHistory.builder()
            //     .productOptionId(productOptionId)
            //     .quantity(quantity)
            //     .reason(reason)
            //     .createdAt(LocalDateTime.now())
            //     .build();
            // stockHistoryRepository.save(stockHistory);
        } catch (Exception e) {
            log.error("재고 변경 이력 기록 실패: productOptionId={}, quantity={}, reason={}", 
                    productOptionId, quantity, reason, e);
        }
    }
}

