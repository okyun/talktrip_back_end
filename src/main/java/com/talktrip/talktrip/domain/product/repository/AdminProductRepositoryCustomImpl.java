package com.talktrip.talktrip.domain.product.repository;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.JPQLQuery;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.talktrip.talktrip.domain.member.entity.QMember;
import com.talktrip.talktrip.domain.product.entity.*;
import com.talktrip.talktrip.global.entity.QCountry;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.regex.Pattern;

@RequiredArgsConstructor
public class AdminProductRepositoryCustomImpl implements AdminProductRepositoryCustom {

    private final JPAQueryFactory queryFactory;

    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_DELETED = "DELETED";

    private static final QProduct PRODUCT = QProduct.product;
    private static final QCountry COUNTRY = QCountry.country;
    private static final QProductOption PRODUCT_OPTION = QProductOption.productOption;
    private static final QProductImage PRODUCT_IMAGE = QProductImage.productImage;
    private static final QHashTag HASH_TAG = QHashTag.hashTag;
    private static final QMember MEMBER = QMember.member;

    private static final Pattern WS = Pattern.compile("\\s+");

    private static List<String> tokensOf(String keyword) {
        if (keyword == null || keyword.isBlank()) return List.of();
        return WS.splitAsStream(keyword.trim())
                .toList();
    }

    private static List<String> distinctTokensLower(List<String> tokens) {
        return tokens.stream()
                .map(s -> s.toLowerCase(Locale.ROOT))
                .distinct()
                .toList();
    }

    private static String normalizeStatus(String status) {
        if (status == null || status.isBlank()) return "";
        return status.trim().toUpperCase(Locale.ROOT);
    }

    private BooleanBuilder keywordFilter(List<String> rawTokens, QProduct p, QHashTag h) {
        List<String> distinct = distinctTokensLower(rawTokens);

        BooleanBuilder andAllTokens = new BooleanBuilder();
        for (String kw : distinct) {
            QCountry c2 = new QCountry("countryForKeyword");
            BooleanExpression perToken = Expressions.FALSE
                    .or(p.productName.startsWith(kw))
                    .or(p.description.startsWith(kw))
                    .or(
                            JPAExpressions.selectOne()
                                    .from(c2)
                                    .where(c2.eq(p.country)
                                            .and(c2.name.startsWith(kw)))
                                    .exists()
                    )
                    .or(
                            JPAExpressions.selectOne()
                                    .from(h)
                                    .where(h.product.eq(p)
                                            .and(h.hashtag.startsWith(kw)))
                                    .exists()
                    );
            andAllTokens.and(perToken);
        }

        return andAllTokens;
    }

    private JPQLQuery<Integer> totalStockQuery(QProduct p, QProductOption o) {
        return JPAExpressions
                .select(o.stock.sum().coalesce(0))
                .from(o)
                .where(o.product.eq(p));
    }

    private void applyOrderBySeller(JPAQuery<Product> query, Pageable pageable,
                                    QProduct p, QProductOption o) {
        if (pageable.getSort().isUnsorted()) {
            query.orderBy(p.updatedAt.desc());
            return;
        }
        for (Sort.Order s : pageable.getSort()) {
            String prop = s.getProperty();
            Order dir = s.isDescending() ? Order.DESC : Order.ASC;
            switch (prop) {
                case "productName" -> query.orderBy(new OrderSpecifier<>(dir, p.productName));
                case "updatedAt" -> query.orderBy(new OrderSpecifier<>(dir, p.updatedAt));
                case "totalStock" -> {
                    JPQLQuery<Integer> q = totalStockQuery(p, o);
                    query.orderBy(new OrderSpecifier<>(dir, q));
                }
                default -> throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Unsupported sort property: " + prop
                );
            }
        }
    }

    @Override
    public Page<Product> findSellerProducts(Long sellerId, String status, String keyword, Pageable pageable) {
        QProduct p = PRODUCT;

        BooleanBuilder where = new BooleanBuilder();
        where.and(p.member.Id.eq(sellerId));

        String st = normalizeStatus(status);
        switch (st) {
            case STATUS_ACTIVE -> where.and(p.deleted.isFalse());
            case STATUS_DELETED -> where.and(p.deleted.isTrue());
            case "ALL" -> {}
            default -> throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Unsupported status: " + status
            );
        }

        List<String> tokens = tokensOf(keyword);
        where.and(keywordFilter(tokens, p, HASH_TAG));

        JPAQuery<Product> dataQuery = queryFactory
                .select(p)
                .from(p)
                .where(where);

        applyOrderBySeller(dataQuery, pageable, p, PRODUCT_OPTION);

        List<Product> content = dataQuery
                .offset(pageable.getOffset())
                .limit(pageable.getPageSize())
                .fetch();

        long total = Objects.requireNonNullElse(
                queryFactory
                        .select(p.id.count())
                        .from(p)
                        .where(where)
                        .fetchOne(),
                0L
        );

        return new PageImpl<>(content, pageable, total);
    }

    @Override
    @Transactional(readOnly = true)
    public Product findProductWithAllDetailsById(Long productId) {
        QProduct p = PRODUCT;
        QProductOption o = PRODUCT_OPTION;
        QProductImage i = PRODUCT_IMAGE;
        QHashTag h = HASH_TAG;
        QCountry c = COUNTRY;
        QMember m = MEMBER;

        List<Product> products = queryFactory
                .selectFrom(p)
                .leftJoin(p.country, c).fetchJoin()
                .leftJoin(p.member, m).fetchJoin()
                .leftJoin(p.productOptions, o).fetchJoin()
                .leftJoin(p.images, i).fetchJoin()
                .leftJoin(p.hashtags, h).fetchJoin()
                .where(p.id.eq(productId))
                .fetch();

        if (products.isEmpty()) {
            return null;
        }

        Product product = products.get(0);

        product.getImages().sort(java.util.Comparator.comparing(ProductImage::getSortOrder));

        return product;
    }
}


