package com.talktrip.talktrip.domain.like.service;

import com.talktrip.talktrip.domain.like.entity.Like;
import com.talktrip.talktrip.domain.like.repository.LikeRepository;
import com.talktrip.talktrip.domain.member.entity.Member;
import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.domain.product.dto.response.ProductSummaryResponse;
import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.repository.ProductRepository;
import com.talktrip.talktrip.domain.review.entity.Review;
import com.talktrip.talktrip.global.exception.ErrorCode;
import com.talktrip.talktrip.global.exception.MemberException;
import com.talktrip.talktrip.global.exception.ProductException;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class LikeService {

    private final LikeRepository likeRepository;
    private final ProductRepository productRepository;
    private final MemberRepository memberRepository;

    @CacheEvict(cacheNames = "product", allEntries = true)
    @Transactional
    public void toggleLike(Long productId, Long memberId) {
        if (likeRepository.existsByProductIdAndMemberId(productId, memberId)) {
            likeRepository.deleteByProductIdAndMemberId(productId, memberId);
            return;
        }

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ProductException(ErrorCode.PRODUCT_NOT_FOUND));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new MemberException(ErrorCode.USER_NOT_FOUND));

        likeRepository.save(Like.builder()
                .product(product)
                .member(member)
                .build());
    }

    @Transactional(readOnly = true)
    public Page<ProductSummaryResponse> getLikedProducts(Long memberId, Pageable pageable) {
        memberRepository.findById(memberId)
                .orElseThrow(() -> new MemberException(ErrorCode.USER_NOT_FOUND));

        Page<Like> likes = likeRepository.findByMemberId(memberId, pageable);

        return likes.map(like -> {
            Product product = like.getProduct();
            float avgStar = (float) product.getReviews().stream()
                    .mapToDouble(Review::getReviewStar)
                    .average()
                    .orElse(0.0);
            return ProductSummaryResponse.from(product, avgStar, true);
        });
    }
}
