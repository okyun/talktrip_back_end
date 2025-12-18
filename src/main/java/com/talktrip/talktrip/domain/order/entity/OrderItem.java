package com.talktrip.talktrip.domain.order.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "order_items")
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "order_id")
    private Order order;

    // 스냅샷 필드들 (기존 연관관계 제거)
    @Column(name = "product_id")
    private Long productId; // 참조용 ID만 저장
    
    @Column(name = "product_name")
    private String productName;
    
    @Column(name = "product_thumbnail_url")
    private String productThumbnailUrl;
    
    @Column(name = "product_price")
    private int productPrice;
    
    @Column(name = "product_option_id")
    private Long productOptionId;
    
    @Column(name = "option_name")
    private String optionName;
    
    @Column(name = "option_price")
    private int optionPrice;
    
    @Column(name = "option_discount_price")
    private int optionDiscountPrice;
    
    @Column(name = "start_date")
    private LocalDate startDate;

    private int quantity;
    private int price; // 주문 당시 실제 결제 가격

    public void setOrder(Order order) {
        this.order = order;
    }

    public static OrderItem createOrderItem(
            Long productId, String productName, String productThumbnailUrl, int productPrice,
            Long productOptionId, String optionName, int optionPrice, int optionDiscountPrice, 
            LocalDate startDate, int quantity, int price) {
        OrderItem orderItem = new OrderItem();
        orderItem.productId = productId;
        orderItem.productName = productName;
        orderItem.productThumbnailUrl = productThumbnailUrl;
        orderItem.productPrice = productPrice;
        orderItem.productOptionId = productOptionId;
        orderItem.optionName = optionName;
        orderItem.optionPrice = optionPrice;
        orderItem.optionDiscountPrice = optionDiscountPrice;
        orderItem.startDate = startDate;
        orderItem.quantity = quantity;
        orderItem.price = price;
        return orderItem;
    }
}