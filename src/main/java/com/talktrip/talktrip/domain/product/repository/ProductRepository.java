package com.talktrip.talktrip.domain.product.repository;

import com.talktrip.talktrip.domain.product.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long>, AdminProductRepositoryCustom, ProductSearchRepositoryCustom {

    @Query(value = "SELECT * FROM product WHERE id = :id", nativeQuery = true)
    Optional<Product> findByIdIncludingDeleted(@Param("id") Long id);

    @Query("select p from Product p where p.id = :id and p.member.Id = :sellerId")
    Optional<Product> findByIdAndMemberIdIncludingDeleted(@Param("id") Long id, @Param("sellerId") Long sellerId);

    @Query("""
            SELECT DISTINCT p FROM Product p
            LEFT JOIN FETCH p.country
            LEFT JOIN FETCH p.member
            WHERE p.id IN :productIds
            """)
    List<Product> findProductSummariesByIds(@Param("productIds") List<Long> productIds);
}
