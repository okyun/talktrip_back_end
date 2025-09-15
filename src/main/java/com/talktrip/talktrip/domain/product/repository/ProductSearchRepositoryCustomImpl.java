package com.talktrip.talktrip.domain.product.repository;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.talktrip.talktrip.domain.like.entity.QLike;
import com.talktrip.talktrip.domain.product.dto.ProductWithAvgStarAndLike;
import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.entity.QProduct;
import com.talktrip.talktrip.domain.product.entity.QProductOption;
import com.talktrip.talktrip.domain.review.entity.QReview;
import com.talktrip.talktrip.global.entity.QCountry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.*;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ProductSearchRepositoryCustomImpl implements ProductSearchRepositoryCustom {

    private final JPAQueryFactory queryFactory;
    private final EntityManager entityManager;

    private static final QProduct PRODUCT = QProduct.product;
    private static final QCountry COUNTRY = QCountry.country;
    private static final QProductOption PRODUCT_OPTION = QProductOption.productOption;
    private static final QReview REVIEW = QReview.review;
    private static final QLike LIKE = QLike.like;

    private static final String ALL_COUNTRIES = "전체";
    private static final Pattern WS = Pattern.compile("\\s+");

    private static List<String> tokensOf(String keyword) {
        if (keyword == null || keyword.isBlank()) return List.of();
        return WS.splitAsStream(keyword.trim()).toList();
    }
    private static boolean containsCJK(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            var us = Character.UnicodeScript.of(ch);
            if (us == Character.UnicodeScript.HANGUL || us == Character.UnicodeScript.HAN
                    || us == Character.UnicodeScript.HIRAGANA || us == Character.UnicodeScript.KATAKANA) return true;
        }
        return false;
    }

    private static List<String> sanitizeTokens(List<String> tokens) {
        if (tokens == null || tokens.isEmpty()) return List.of();
        return tokens.stream()
                .map(s -> s == null ? "" : s.trim())
                .filter(s -> {
                    if (s.isEmpty()) return false;
                    boolean cjk = containsCJK(s);
                    return cjk ? s.length() >= 2 : s.length() >= 3;
                })
                .map(s -> s.toLowerCase(Locale.ROOT))
                .distinct()
                .limit(5)
                .toList();
    }

    private static String toEscapedContainsPattern(String token) {
        String escaped = token.replace("!", "!!").replace("%", "!%").replace("_", "!_");
        return "%" + escaped + "%";
    }

    private static String buildBooleanFulltextQuery(List<String> tokens) {
        if (tokens == null || tokens.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String t : tokens) {
            if (t == null || t.isBlank()) continue;
            sb.append('+').append(t);
            sb.append(' ');
        }
        return sb.toString().trim();
    }

    private boolean isExistingCountryName(String name) {
        try {
            Query q = entityManager.createNativeQuery("SELECT 1 FROM country WHERE name = :n LIMIT 1");
            q.setParameter("n", name);
            return !q.getResultList().isEmpty();
        } catch (Exception e) { return false; }
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<ProductWithAvgStarAndLike> findByIdWithDetailsAndAvgStarAndLike(Long productId, Long memberId) {
        QProduct p = PRODUCT;
        QProductOption o = PRODUCT_OPTION;

        BooleanBuilder where = new BooleanBuilder();
        where.and(p.id.eq(productId));
        where.and(p.deleted.isFalse());
        LocalDate tomorrow = LocalDate.now().plusDays(1);

        BooleanExpression hasFutureStock = JPAExpressions.selectOne()
                .from(new QProductOption("opt_future"))
                .where(PRODUCT_OPTION.product.eq(p)
                        .and(PRODUCT_OPTION.stock.gt(0))
                        .and(PRODUCT_OPTION.startDate.goe(tomorrow)))
                .limit(1)
                .exists();
        where.and(hasFutureStock);

        ProductWithAvgStarAndLike result = buildProductWithAvgStarAndLikeQuery(
                p, COUNTRY, o, REVIEW, where, memberId, tomorrow
        ).having(o.discountPrice.min().isNotNull()).fetchFirst();

        return Optional.ofNullable(result);
    }

    private JPAQuery<ProductWithAvgStarAndLike> buildProductWithAvgStarAndLikeQuery(
            QProduct p, QCountry c, QProductOption o, QReview r,
            BooleanBuilder where, Long memberId, LocalDate tomorrow
    ) {
        return queryFactory
                .select(com.querydsl.core.types.Projections.constructor(ProductWithAvgStarAndLike.class,
                        p,
                        r.reviewStar.avg().coalesce(0.0),
                        memberId == null ? Expressions.constant(false) :
                                JPAExpressions.selectOne().from(LIKE)
                                        .where(LIKE.productId.eq(p.id).and(LIKE.memberId.eq(memberId))).exists(),
                        o.discountPrice.min()
                ))
                .from(p)
                .leftJoin(p.country, c)
                .leftJoin(o).on(o.product.eq(p).and(o.startDate.goe(tomorrow)).and(o.stock.gt(0)))
                .leftJoin(r).on(r.product.eq(p))
                .where(where)
                .groupBy(p.id, p.productName, p.description, p.thumbnailImageUrl,
                        p.thumbnailImageHash, p.member, p.country, p.deleted, p.deletedAt, p.createdAt, p.updatedAt);
    }

    private Map<Long, Double> getAverageStarsByProductIds(List<Long> productIds) {
        if (productIds.isEmpty()) return Map.of();
        return queryFactory
                .select(REVIEW.product.id, REVIEW.reviewStar.avg().coalesce(0.0))
                .from(REVIEW)
                .where(REVIEW.product.id.in(productIds))
                .groupBy(REVIEW.product.id)
                .fetch()
                .stream()
                .collect(Collectors.toMap(
                        t -> t.get(REVIEW.product.id),
                        t -> t.get(REVIEW.reviewStar.avg().coalesce(0.0))
                ));
    }

    private Map<Long, Integer> getMinDiscountPriceByProductIds(List<Long> productIds) {
        if (productIds.isEmpty()) return Map.of();
        LocalDate tomorrow = LocalDate.now().plusDays(1);
        return queryFactory
                .select(PRODUCT_OPTION.product.id, PRODUCT_OPTION.discountPrice.min())
                .from(PRODUCT_OPTION)
                .where(PRODUCT_OPTION.product.id.in(productIds)
                        .and(PRODUCT_OPTION.stock.gt(0))
                        .and(PRODUCT_OPTION.startDate.goe(tomorrow)))
                .groupBy(PRODUCT_OPTION.product.id)
                .fetch()
                .stream()
                .collect(Collectors.toMap(
                        t -> t.get(PRODUCT_OPTION.product.id),
                        t -> t.get(PRODUCT_OPTION.discountPrice.min())
                ));
    }

    private Set<Long> getLikedProductIdSet(List<Long> productIds, Long memberId) {
        if (memberId == null || productIds.isEmpty()) return Set.of();
        return new HashSet<>(queryFactory.select(LIKE.productId)
                .from(LIKE)
                .where(LIKE.productId.in(productIds).and(LIKE.memberId.eq(memberId))).fetch());
    }

    private List<Product> getProductsByIdsPreservingOrder(List<Long> productIds) {
        if (productIds.isEmpty()) return List.of();
        List<Product> fetched = queryFactory
                .selectFrom(PRODUCT)
                .leftJoin(PRODUCT.country, COUNTRY).fetchJoin()
                .where(PRODUCT.id.in(productIds))
                .fetch();
        Map<Long, Product> map = fetched.stream().collect(Collectors.toMap(Product::getId, x -> x));
        List<Product> ordered = new ArrayList<>(productIds.size());
        for (Long id : productIds) {
            Product p = map.get(id);
            if (p != null) ordered.add(p);
        }
        return ordered;
    }

    @Override
    @Transactional(readOnly = true)
    public Slice<ProductWithAvgStarAndLike> searchProductsWithAvgStarAndLikeCursor(
            String keyword,
            String countryName,
            Long memberId,
            java.time.LocalDateTime lastUpdatedAt,
            Long lastId,
            Double lastAvgStar,
            Integer lastMinPrice,
            int size,
            String sortKey
    ) {
        List<Long> ids = findProductIdsForSearchCursor(
                keyword, countryName, lastUpdatedAt, lastId, lastAvgStar, lastMinPrice, size, sortKey);
        boolean hasNext = ids.size() > size;
        if (hasNext) ids = ids.subList(0, size);

        List<Product> products = getProductsByIdsPreservingOrder(ids);
        Map<Long, Double> avgStars = getAverageStarsByProductIds(ids);
        Map<Long, Integer> minPrices = getMinDiscountPriceByProductIds(ids);
        Set<Long> likedSet = getLikedProductIdSet(ids, memberId);

        List<ProductWithAvgStarAndLike> content = products.stream()
                .map(p -> new ProductWithAvgStarAndLike(
                        p,
                        avgStars.getOrDefault(p.getId(), 0.0),
                        likedSet.contains(p.getId()),
                        minPrices.get(p.getId())
                ))
                .toList();

        return new SliceImpl<>(content, Pageable.ofSize(size), hasNext);
    }

    private List<Long> findProductIdsForSearchCursor(
            String keyword, String countryName,
            java.time.LocalDateTime lastUpdatedAt, Long lastId,
            Double lastAvgStar, Integer lastMinPrice,
            int size, String sortKey
    ) {
        String cn = Optional.ofNullable(countryName).map(String::trim).orElse("");
        List<String> tokens = sanitizeTokens(tokensOf(keyword));
        boolean hasKeyword = !tokens.isEmpty();
        boolean useFts = hasKeyword && tokens.size() == 1;
        boolean hasCountryFilter = !cn.isEmpty() && !ALL_COUNTRIES.equals(cn);

        if (!hasCountryFilter && tokens.size() == 1 && isExistingCountryName(tokens.get(0))) {
            hasCountryFilter = true;
            cn = tokens.get(0);
        }

        String[] parts = (sortKey == null ? "updatedAt,desc" : sortKey).split(",");
        String key = parts[0];
        boolean desc = parts.length < 2 || "desc".equalsIgnoreCase(parts[1]);

        // 추가팁 1) 다단어 0-hit 단락: 하나라도 문서에 없으면 즉시 0건 반환 (기존 흐름 영향 없음)
        if (hasKeyword && tokens.size() >= 2) {
            try {
                for (String t : tokens) {
                    if (t == null || t.isBlank()) continue;
                    StringBuilder ex = new StringBuilder();
                    ex.append("SELECT 1 FROM product p WHERE p.deleted = false AND p.has_future_stock = 1 ");
                    if (hasCountryFilter) ex.append("AND p.country_name_cached = :countryName ");
                    ex.append("AND MATCH(p.search_text) AGAINST (:q IN BOOLEAN MODE) LIMIT 1");
                    Query eq = entityManager.createNativeQuery(ex.toString());
                    if (hasCountryFilter) eq.setParameter("countryName", cn);
                    eq.setParameter("q", "+" + t);
                    if (eq.getResultList().isEmpty()) {
                        return java.util.Collections.emptyList();
                    }
                }
            } catch (Exception ignore) { /* FTS 불가 시 단락 검사 생략 */ }
        }

        boolean isSingleToken = hasKeyword && tokens.size() == 1;
        boolean restrictBySeed = false;
        String seedToken = null;
        List<String> restTokens = List.of();
        int preLimit = Math.max(size * 200, 2000);
        int probeLimit = Math.min(preLimit, 1000);

        if (hasKeyword && tokens.size() >= 2) {
            restrictBySeed = false;
            seedToken = null;
            restTokens = List.of();
        }

        if (isSingleToken && !restrictBySeed) {
            try {
                StringBuilder probe = new StringBuilder();
                probe.append("SELECT 1 FROM product p WHERE p.deleted = false AND p.has_future_stock = 1 ");
                if (hasCountryFilter) probe.append("AND p.country_name_cached = :countryName ");
                probe.append("AND MATCH(p.search_text) AGAINST (:q IN BOOLEAN MODE) LIMIT :plim");
                Query pq = entityManager.createNativeQuery(probe.toString());
                if (hasCountryFilter) pq.setParameter("countryName", cn);
                pq.setParameter("q", buildBooleanFulltextQuery(tokens));
                pq.setParameter("plim", probeLimit);
                int hits = pq.getResultList().size();
                if (hits >= probeLimit) {
                    restrictBySeed = true;
                    seedToken = tokens.get(0);
                }
            } catch (Exception ignore) {}
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT p.id FROM product p ");
        if (restrictBySeed) {
            sql.append("JOIN (")
                    .append(" SELECT id FROM product WHERE deleted = false AND has_future_stock = 1 ");
            if (hasCountryFilter) sql.append(" AND country_name_cached = :countryName ");
            sql.append(" AND MATCH(search_text) AGAINST (:seedBool IN BOOLEAN MODE) ")
                    .append(" LIMIT :preLimit ")
                    .append(") s ON s.id = p.id ");
        }
        sql.append("WHERE p.deleted = false AND p.has_future_stock = 1 ");
        if (hasCountryFilter) {
            sql.append("AND p.country_name_cached = :countryName ");
        }
        if (restrictBySeed) {
            if (!restTokens.isEmpty()) {
                for (int i = 0; i < restTokens.size(); i++) {
                    sql.append("AND p.search_text LIKE :restLike").append(i).append(" ESCAPE '!' ");
                }
            }
        } else {
            if (useFts) {
                sql.append("AND MATCH(p.search_text) AGAINST (:ftq IN BOOLEAN MODE) ");
            } else if (hasKeyword) {
                for (int i = 0; i < tokens.size(); i++) {
                    sql.append("AND p.search_text LIKE :fbkw").append(i).append(" ESCAPE '!' ");
                }
            }
        }

        boolean needRating = "rating".equals(key);
        boolean needMinPrice = "minPrice".equals(key);
        if (needRating && lastAvgStar != null && lastUpdatedAt != null && lastId != null) {
            sql.append(" AND (p.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                    .append("  OR (p.avg_review_star_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                    .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
        } else if (needMinPrice && lastMinPrice != null && lastUpdatedAt != null && lastId != null) {
            sql.append(" AND (p.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                    .append("  OR (p.min_discount_price_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                    .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
        } else if ("updatedAt".equals(key) && lastUpdatedAt != null && lastId != null) {
            sql.append(" AND (p.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                    .append("  OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))");
        }

        if (needRating) {
            sql.append(" ORDER BY p.avg_review_star_cached ").append(desc ? "DESC" : "ASC")
                    .append(", p.updated_at DESC, p.id DESC ");
        } else if (needMinPrice) {
            sql.append(" ORDER BY p.min_discount_price_cached ").append(desc ? "DESC" : "ASC")
                    .append(", p.updated_at DESC, p.id DESC ");
        } else {
            sql.append(" ORDER BY p.updated_at ").append(desc ? "DESC" : "ASC")
                    .append(", p.id DESC ");
        }

        sql.append(" LIMIT :limitPlusOne");

        Query q = entityManager.createNativeQuery(sql.toString(), Long.class);
        q.setParameter("limitPlusOne", size + 1);
        if (hasCountryFilter) q.setParameter("countryName", cn);
        if (restrictBySeed) {
            q.setParameter("seedBool", "+" + seedToken);
            q.setParameter("preLimit", preLimit);
            if (!restTokens.isEmpty()) {
                for (int i = 0; i < restTokens.size(); i++) {
                    q.setParameter("restLike" + i, toEscapedContainsPattern(restTokens.get(i)));
                }
            }
        } else {
            if (useFts) {
                q.setParameter("ftq", buildBooleanFulltextQuery(tokens));
            } else if (hasKeyword) {
                for (int i = 0; i < tokens.size(); i++) {
                    q.setParameter("fbkw" + i, toEscapedContainsPattern(tokens.get(i)));
                }
            }
        }
        if (lastUpdatedAt != null) q.setParameter("lastUpdatedAt", java.sql.Timestamp.valueOf(lastUpdatedAt));
        if (lastId != null) q.setParameter("lastId", lastId);
        if (("rating".equals(key) && lastAvgStar != null) || ("minPrice".equals(key) && lastMinPrice != null)) {
            q.setParameter("lastM", "rating".equals(key) ? lastAvgStar : lastMinPrice);
        }
        try {
            @SuppressWarnings("unchecked")
            List<Long> ids = (List<Long>) q.getResultList();
            return ids;
        } catch (Exception ex) {
            String msg = ex.getMessage();
            boolean ftsCache = msg != null && msg.contains("FTS query exceeds result cache limit");
            if (ftsCache) {
                StringBuilder fb = new StringBuilder();
                fb.append("SELECT p.id FROM product p WHERE p.deleted = false AND p.has_future_stock = 1 ");
                if (hasCountryFilter) fb.append("AND p.country_name_cached = :countryName ");
                if (useFts && hasKeyword) {
                    fb.append("AND MATCH(p.search_text) AGAINST (:ftNat IN NATURAL LANGUAGE MODE) ");
                } else if (hasKeyword) {
                    for (int i = 0; i < tokens.size(); i++) {
                        fb.append("AND p.search_text LIKE :fbkw").append(i).append(" ESCAPE '!'").append(' ');
                    }
                }

                if (needRating && lastAvgStar != null && lastUpdatedAt != null && lastId != null) {
                    fb.append(" AND (p.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                            .append("  OR (p.avg_review_star_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                            .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
                } else if (needMinPrice && lastMinPrice != null && lastUpdatedAt != null && lastId != null) {
                    fb.append(" AND (p.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                            .append("  OR (p.min_discount_price_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                            .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
                } else if ("updatedAt".equals(key) && lastUpdatedAt != null && lastId != null) {
                    fb.append(" AND (p.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                            .append("  OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))");
                }

                if (needRating) {
                    fb.append(" ORDER BY p.avg_review_star_cached ").append(desc ? "DESC" : "ASC")
                            .append(", p.updated_at DESC, p.id DESC ");
                } else if (needMinPrice) {
                    fb.append(" ORDER BY p.min_discount_price_cached ").append(desc ? "DESC" : "ASC")
                            .append(", p.updated_at DESC, p.id DESC ");
                } else {
                    fb.append(" ORDER BY p.updated_at ").append(desc ? "DESC" : "ASC")
                            .append(", p.id DESC ");
                }
                fb.append(" LIMIT :limitPlusOne");

                Query q2 = entityManager.createNativeQuery(fb.toString(), Long.class);
                q2.setParameter("limitPlusOne", size + 1);
                if (hasCountryFilter) q2.setParameter("countryName", cn);
                if (useFts && hasKeyword) q2.setParameter("ftNat", String.join(" ", tokens));
                else if (hasKeyword) {
                    for (int i = 0; i < tokens.size(); i++) {
                        q2.setParameter("fbkw" + i, toEscapedContainsPattern(tokens.get(i)));
                    }
                }
                if (lastUpdatedAt != null) q2.setParameter("lastUpdatedAt", java.sql.Timestamp.valueOf(lastUpdatedAt));
                if (lastId != null) q2.setParameter("lastId", lastId);
                if (("rating".equals(key) && lastAvgStar != null) || ("minPrice".equals(key) && lastMinPrice != null)) {
                    q2.setParameter("lastM", "rating".equals(key) ? lastAvgStar : lastMinPrice);
                }
                @SuppressWarnings("unchecked")
                List<Long> ids2 = (List<Long>) q2.getResultList();
                return ids2;
            }
            throw ex;
        }
    }
}
