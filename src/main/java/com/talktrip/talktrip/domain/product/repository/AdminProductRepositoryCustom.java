package com.talktrip.talktrip.domain.product.repository;

import com.talktrip.talktrip.domain.product.entity.Product;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

public interface AdminProductRepositoryCustom {
    Page<Product> findSellerProducts(Long sellerId, String status, String keyword, Pageable pageable);
    Product findProductWithAllDetailsById(Long productId);
}


