package com.talktrip.talktrip.domain.product.repository;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.JPQLQuery;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.entity.QHashTag;
import com.talktrip.talktrip.domain.product.entity.QProduct;
import com.talktrip.talktrip.domain.product.entity.QProductOption;
import com.talktrip.talktrip.domain.review.entity.QReview;
import com.talktrip.talktrip.global.entity.QCountry;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
public class ProductRepositoryImpl implements ProductRepositoryCustom {

    private final JPAQueryFactory queryFactory;

    private static final Set<String> ALLOWED_SORT_KEYS = Set.of(
            "updatedAt", "productName", "discountPrice", "averageStar"
    );

    private static final Set<String> ALLOWED_SELLER_SORT = Set.of(
            "productName", "price", "discountPrice", "totalStock", "updatedAt"
    );

    // 부분문자열 등장 횟수 >= req (lower/replace)
    private static BooleanExpression occGoe(Path<String> col, String kwLower, int req) {
        return Expressions.numberTemplate(Integer.class,
                "((length(lower({0})) - length(function('replace', lower({0}), {1}, ''))) / length({1}))",
                col, Expressions.constant(kwLower)
        ).goe(req);
    }

    private BooleanBuilder keywordWhere(List<String> keywords, QProduct p, QCountry c, QHashTag hSub) {
        Map<String, Long> need = keywords.stream()
                .filter(s -> s != null && !s.isBlank())
                .map(s -> s.toLowerCase(java.util.Locale.ROOT))
                .collect(Collectors.groupingBy(s -> s, Collectors.counting()));

        BooleanBuilder where = new BooleanBuilder();
        for (Map.Entry<String, Long> e : need.entrySet()) {
            String kw = e.getKey();
            int req = e.getValue().intValue();

            BooleanExpression perKeyword =
                    occGoe(p.productName, kw, req)
                            .or(occGoe(p.description, kw, req))
                            .or(occGoe(c.name, kw, req))
                            .or(JPAExpressions.selectOne()
                                    .from(hSub)
                                    .where(hSub.product.eq(p)
                                            .and(occGoe(hSub.hashtag, kw, req)))
                                    .exists());

            where.and(perKeyword);
        }
        return where;
    }

    private BooleanExpression hasFutureStock(QProduct p, QProductOption o) {
        return JPAExpressions.selectOne()
                .from(o)
                .where(o.product.eq(p)
                       // .and(o.startDate.goe(LocalDate.now()))
                        .and(o.stock.gt(0)))
                .exists();
    }

    private JPQLQuery<Integer> minDiscountPriceQuery(QProduct p, QProductOption o) {
        return JPAExpressions
                .select(o.discountPrice.min())
                .from(o)
                .where(o.product.eq(p));
    }

    private JPQLQuery<Double> avgStarQuery(QProduct p, QReview r) {
        return JPAExpressions
                .select(r.reviewStar.avg().coalesce(0.0))
                .from(r)
                .where(r.product.eq(p));
    }

    private void applyOrderBy(JPAQuery<Product> query, Pageable pageable,
                              QProduct p, QProductOption o, QReview r) {
        if (pageable.getSort().isUnsorted()) {
            query.orderBy(p.updatedAt.desc());
            return;
        }

        for (Sort.Order s : pageable.getSort()) {
            String prop = s.getProperty();
            Order dir = s.isDescending() ? Order.DESC : Order.ASC;

            if (!ALLOWED_SORT_KEYS.contains(prop)) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Unsupported sort property: " + prop
                );
            }

            switch (prop) {
                case "updatedAt" -> query.orderBy(new OrderSpecifier<>(dir, p.updatedAt));
                case "productName" -> query.orderBy(new OrderSpecifier<>(dir, p.productName));
                case "discountPrice" -> {
                    JPQLQuery<Integer> minDiscountQ = minDiscountPriceQuery(p, o);
                    query.orderBy(new OrderSpecifier<>(dir, minDiscountQ, OrderSpecifier.NullHandling.NullsLast));
                }
                case "averageStar" -> {
                    JPQLQuery<Double> avgStarQ = avgStarQuery(p, r);
                    query.orderBy(new OrderSpecifier<>(dir, avgStarQ));
                }
            }
        }
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

            if (!ALLOWED_SELLER_SORT.contains(prop)) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Unsupported sort property: " + prop
                );
            }

            switch (prop) {
                case "productName" -> query.orderBy(new OrderSpecifier<>(dir, p.productName));
                case "updatedAt"   -> query.orderBy(new OrderSpecifier<>(dir, p.updatedAt));
                case "totalStock" -> {
                    JPQLQuery<Integer> q = totalStockQuery(p, o);
                    query.orderBy(new OrderSpecifier<>(dir, q));
                }
            }
        }
    }

    private Page<Product> fetchPage(BooleanBuilder where, Pageable pageable) {
        QProduct p = QProduct.product;
        QCountry c = QCountry.country;
        QProductOption o = QProductOption.productOption;
        QReview r = QReview.review;

        JPAQuery<Product> dataQuery = queryFactory
                .selectDistinct(p)
                .from(p)
                .leftJoin(p.country, c).fetchJoin()
                .where(where);

        applyOrderBy(dataQuery, pageable, p, o, r);

        List<Product> content = dataQuery
                .offset(pageable.getOffset())
                .limit(pageable.getPageSize())
                .fetch();

        Long total = queryFactory
                .select(p.id.countDistinct())
                .from(p)
                .leftJoin(p.country, c)
                .where(where)
                .fetchOne();

        return new PageImpl<>(content, pageable, total == null ? 0 : total);
    }

    @Override
    public Page<Product> searchByKeywords(List<String> keywords, String countryName, Pageable pageable) {
        QProduct p = QProduct.product;
        QCountry c = QCountry.country;
        QHashTag hSub = new QHashTag("hSub");
        QProductOption o = QProductOption.productOption;

        BooleanBuilder where = keywordWhere(keywords, p, c, hSub);

        if (countryName != null && !countryName.isBlank() && !"전체".equals(countryName)) {
            where.and(c.name.equalsIgnoreCase(countryName.trim()));
        }
        where.and(hasFutureStock(p, o));

        return fetchPage(where, pageable);
    }

    @Override
    public Page<Product> findVisibleProducts(String countryName, Pageable pageable) {
        QProduct p = QProduct.product;
        QCountry c = QCountry.country;
        QProductOption o = QProductOption.productOption;

        BooleanBuilder where = new BooleanBuilder();
        if (countryName != null && !countryName.isBlank() && !"전체".equals(countryName)) {
            where.and(c.name.equalsIgnoreCase(countryName.trim()));
        }
        where.and(hasFutureStock(p, o));

        return fetchPage(where, pageable);
    }

    @Override
    public Page<Product> findSellerProducts(Long sellerId, String status, String keyword, Pageable pageable) {
        QProduct p = QProduct.product;
        QProductOption o = QProductOption.productOption;

        BooleanBuilder where = new BooleanBuilder();
        where.and(p.member.Id.eq(sellerId));

        if (status != null && !status.isBlank()) {
            switch (status.trim().toUpperCase()) {
                case "ACTIVE" -> where.and(p.deleted.isFalse());
                case "DELETED" -> where.and(p.deleted.isTrue());
                default -> throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Unsupported status: " + status
                );
            }
        }

        if (keyword != null && !keyword.isBlank()) {
            for (String tok : keyword.trim().split("\\s+")) {
                if (tok.isBlank()) continue;
                where.and(
                        p.productName.containsIgnoreCase(tok)
                                .or(p.description.containsIgnoreCase(tok))
                );
            }
        }

        JPAQuery<Product> dataQuery = queryFactory
                .select(p)
                .from(p)
                .where(where);

        applyOrderBySeller(dataQuery, pageable, p, o);

        List<Product> content = dataQuery
                .offset(pageable.getOffset())
                .limit(pageable.getPageSize())
                .fetch();

        Long total = queryFactory
                .select(p.id.count())
                .from(p)
                .where(where)
                .fetchOne();

        return new PageImpl<>(content, pageable, total == null ? 0 : total);
    }
}
