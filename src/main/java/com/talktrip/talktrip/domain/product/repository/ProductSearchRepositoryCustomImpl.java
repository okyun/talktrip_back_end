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
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.transaction.annotation.Propagation;
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

    // 후보 상/하한 — 필요 시 조정
    private static final int MIN_PRELIMIT = 800;
    private static final int MAX_PRELIMIT = 6000;

    // ===== 희소 토큰 기반 FTS 프리필터링 설정 =====
    // FTS 시드로 선택할 최대 개수
    private static final int MAX_FTS_TOKENS = 4;
    // 토큰 희소도 빠른 추정 시 상한 샘플 수(실쿼리 LIMIT)
    private static final int PROBE_LIMIT = 1000;
    // “희소하다”고 볼 만한 프로브 히트 임계치(튜닝 포인트)
    private static final int PROBE_HIT_TARGET = 300;
    // 환경에 맞게: CJK도 FULLTEXT(n-gram) 인덱스가 있다면 true
    private static final boolean USE_CJK_FTS = true;

    // ---------- token utils ----------
    private static List<String> tokensOf(String keyword) {
        if (keyword == null || keyword.isBlank()) return List.of();
        return WS.splitAsStream(keyword.trim()).toList();
    }
    private static boolean containsCJK(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            Character.UnicodeScript us = Character.UnicodeScript.of(ch);
            if (us == Character.UnicodeScript.HANGUL
                    || us == Character.UnicodeScript.HAN
                    || us == Character.UnicodeScript.HIRAGANA
                    || us == Character.UnicodeScript.KATAKANA) return true;
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
                .limit(5) // 과도한 키워드 남용 방지(최대 5개 유지)
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
            sb.append('+').append(t).append(' ');
        }
        return sb.toString().trim();
    }
    private static boolean cjkLikelyExpensiveForBoolean(String token) {
        return token != null && !token.isBlank() && containsCJK(token);
    }

    private boolean isExistingCountryName(String name) {
        try {
            Query q = entityManager.createNativeQuery("SELECT 1 FROM country WHERE name = :n LIMIT 1");
            q.setParameter("n", name);
            return !q.getResultList().isEmpty();
        } catch (Exception e) { return false; }
    }

    // ====== 빠른 히트수 프로브(저비용) & 제로-히트 확인 ======
    private int probeHits(String token, boolean hasCountryFilter, String countryName) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT p.id FROM product p WHERE p.deleted=false AND p.has_future_stock=1 ");
            if (hasCountryFilter) sb.append("AND p.country_name_cached = :countryName ");
            sb.append("AND INSTR(p.search_text, :tok) > 0 ");
            sb.append("ORDER BY p.updated_at DESC, p.id DESC LIMIT :plim");
            Query q = entityManager.createNativeQuery(sb.toString());
            if (hasCountryFilter) q.setParameter("countryName", countryName);
            q.setParameter("tok", token);
            q.setParameter("plim", PROBE_LIMIT);
            return q.getResultList().size(); // 0..PROBE_LIMIT
        } catch (Exception e) {
            return PROBE_LIMIT; // 실패시 최악치
        }
    }
    private boolean existsTokenFast(String token, boolean hasCountryFilter, String countryName) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT 1 FROM product p WHERE p.deleted=false AND p.has_future_stock=1 ");
            if (hasCountryFilter) sb.append("AND p.country_name_cached = :countryName ");
            sb.append("AND INSTR(p.search_text, :tok) > 0 LIMIT 1");
            Query q = entityManager.createNativeQuery(sb.toString());
            if (hasCountryFilter) q.setParameter("countryName", countryName);
            q.setParameter("tok", token);
            return !q.getResultList().isEmpty();
        } catch (Exception e) {
            // 실패 시 보수적으로 존재한다고 가정(검색 실패 방지)
            return true;
        }
    }

    // ================== 상세 조회 ==================
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED, readOnly = true)
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

    // ================== 검색 ==================
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED, readOnly = true)
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
        boolean single = hasKeyword && tokens.size() == 1;
        boolean hasCountryFilter = !cn.isEmpty() && !ALL_COUNTRIES.equals(cn);

        if (!hasCountryFilter && single && isExistingCountryName(tokens.get(0))) {
            hasCountryFilter = true;
            cn = tokens.get(0);
        }

        String[] parts = (sortKey == null ? "updatedAt,desc" : sortKey).split(",");
        String key = parts[0];
        boolean desc = parts.length < 2 || "desc".equalsIgnoreCase(parts[1]);
        boolean needRating = "rating".equals(key);
        boolean needMinPrice = "minPrice".equals(key);

        boolean useCursorOnRating    = needRating && lastAvgStar != null && lastUpdatedAt != null && lastId != null;
        boolean useCursorOnPrice     = needMinPrice && lastMinPrice != null && lastUpdatedAt != null && lastId != null;
        boolean useCursorOnUpdatedAt = "updatedAt".equals(key) && lastUpdatedAt != null && lastId != null;

        // 키워드 없음 → 캐시 컬럼만으로 정렬/커서
        if (!hasKeyword) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT p.id FROM product p WHERE p.deleted=false AND p.has_future_stock=1 ");
            if (hasCountryFilter) sql.append("AND p.country_name_cached = :countryName ");

            if (useCursorOnRating) {
                sql.append(" AND (p.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                        .append("  OR (p.avg_review_star_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                        .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
            } else if (useCursorOnPrice) {
                sql.append(" AND (p.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                        .append("  OR (p.min_discount_price_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                        .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
            } else if (useCursorOnUpdatedAt) {
                sql.append(" AND (p.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                        .append("  OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))");
            }

            appendOrder(sql, key, desc, needRating, needMinPrice);
            sql.append(" LIMIT :limitPlusOne");

            Query q = entityManager.createNativeQuery(sql.toString());
            q.setParameter("limitPlusOne", size + 1);
            if (hasCountryFilter) q.setParameter("countryName", cn);
            bindCursorParams(q, useCursorOnRating, useCursorOnPrice, useCursorOnUpdatedAt,
                    lastAvgStar, lastMinPrice, lastUpdatedAt, lastId);

            return toLongIds(q.getResultList());
        }

        // ---------- 단일 키워드 ----------
        if (single) {
            String token = tokens.get(0);

            // CJK 단일어는 후보 프리리밋 + INSTR
            if (cjkLikelyExpensiveForBoolean(token)) {
                return runInstrTopNForSingle(token, cn, hasCountryFilter,
                        lastUpdatedAt, lastId, lastAvgStar, lastMinPrice,
                        size, key, desc, needRating, needMinPrice,
                        useCursorOnRating, useCursorOnPrice, useCursorOnUpdatedAt);
            }

            // 비-CJK 단일어: FTS BOOLEAN (빠른 케이스)
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT p.id FROM product p WHERE p.deleted=false AND p.has_future_stock=1 ");
            if (hasCountryFilter) sql.append("AND p.country_name_cached = :countryName ");
            sql.append("AND MATCH(p.search_text) AGAINST (:ftq IN BOOLEAN MODE) ");

            if (useCursorOnRating) {
                sql.append(" AND (p.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                        .append("  OR (p.avg_review_star_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                        .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
            } else if (useCursorOnPrice) {
                sql.append(" AND (p.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                        .append("  OR (p.min_discount_price_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                        .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
            } else if (useCursorOnUpdatedAt) {
                sql.append(" AND (p.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                        .append("  OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))");
            }

            appendOrder(sql, key, desc, needRating, needMinPrice);
            sql.append(" LIMIT :limitPlusOne");

            Query q = entityManager.createNativeQuery(sql.toString());
            q.setParameter("limitPlusOne", size + 1);
            if (hasCountryFilter) q.setParameter("countryName", cn);
            q.setParameter("ftq", buildBooleanFulltextQuery(tokens));
            bindCursorParams(q, useCursorOnRating, useCursorOnPrice, useCursorOnUpdatedAt,
                    lastAvgStar, lastMinPrice, lastUpdatedAt, lastId);

            return toLongIds(q.getResultList());
        }

        // ---------- 다중 키워드 ----------
        // 0) 제로-히트 토큰이 하나라도 있으면 바로 빈 결과 반환(최종 AND 필터 기준)
        for (String t : tokens) {
            if (!existsTokenFast(t, hasCountryFilter, cn)) {
                return List.of();
            }
        }

        // 1) 프로브 기반으로 희소 토큰을 뽑아 FTS 시드로 사용
        List<String> ftsTokens = chooseFtsSeedsByProbe(tokens, hasCountryFilter, cn);

        if (!ftsTokens.isEmpty()) {
            // (A) 희소 토큰들로 FTS 후보 축소 → 최종 AND는 INSTR로 전체 토큰 검증
            StringBuilder cand = new StringBuilder();
            cand.append("SELECT b.id FROM product b WHERE b.deleted=false AND b.has_future_stock=1 ");
            if (hasCountryFilter) cand.append("AND b.country_name_cached = :countryName ");
            cand.append("AND MATCH(b.search_text) AGAINST (:ftq IN BOOLEAN MODE) ");
            if (useCursorOnRating) {
                cand.append(" AND (b.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                        .append("  OR (b.avg_review_star_cached = :lastM AND (b.updated_at < :lastUpdatedAt ")
                        .append("      OR (b.updated_at = :lastUpdatedAt AND b.id < :lastId))) )");
            } else if (useCursorOnPrice) {
                cand.append(" AND (b.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                        .append("  OR (b.min_discount_price_cached = :lastM AND (b.updated_at < :lastUpdatedAt ")
                        .append("      OR (b.updated_at = :lastUpdatedAt AND b.id < :lastId))) )");
            } else if (useCursorOnUpdatedAt) {
                cand.append(" AND (b.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                        .append("  OR (b.updated_at = :lastUpdatedAt AND b.id < :lastId))");
            }

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT p.id FROM product p ")
                    .append("JOIN (").append(cand).append(") s ON s.id = p.id ")
                    .append("WHERE p.deleted=false AND p.has_future_stock=1 ");
            if (hasCountryFilter) sql.append("AND p.country_name_cached = :countryName ");
            for (int i = 0; i < tokens.size(); i++) {
                sql.append(" AND INSTR(p.search_text, :tok").append(i).append(") > 0 ");
            }

            appendOrder(sql, key, desc, needRating, needMinPrice);
            sql.append(" LIMIT :limitPlusOne");

            Query q = entityManager.createNativeQuery(sql.toString());
            q.setParameter("limitPlusOne", size + 1);
            if (hasCountryFilter) q.setParameter("countryName", cn);
            q.setParameter("ftq", buildBooleanFulltextQuery(ftsTokens));
            for (int i = 0; i < tokens.size(); i++) q.setParameter("tok" + i, tokens.get(i));
            bindCursorParams(q, useCursorOnRating, useCursorOnPrice, useCursorOnUpdatedAt,
                    lastAvgStar, lastMinPrice, lastUpdatedAt, lastId);

            return toLongIds(q.getResultList());
        }

        // (B) 희소 토큰 없음 → 기존 안정 경로: LIKE AND (ESCAPE 포함)
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT p.id FROM product p WHERE p.deleted=false AND p.has_future_stock=1 ");
        if (hasCountryFilter) sql.append("AND p.country_name_cached = :countryName ");
        for (int i = 0; i < tokens.size(); i++) {
            sql.append("AND p.search_text LIKE :kw").append(i).append(" ESCAPE '!' ");
        }

        if (useCursorOnRating) {
            sql.append(" AND (p.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                    .append("  OR (p.avg_review_star_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                    .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
        } else if (useCursorOnPrice) {
            sql.append(" AND (p.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                    .append("  OR (p.min_discount_price_cached = :lastM AND (p.updated_at < :lastUpdatedAt ")
                    .append("      OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))) )");
        } else if (useCursorOnUpdatedAt) {
            sql.append(" AND (p.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                    .append("  OR (p.updated_at = :lastUpdatedAt AND p.id < :lastId))");
        }

        appendOrder(sql, key, desc, needRating, needMinPrice);
        sql.append(" LIMIT :limitPlusOne");

        Query q = entityManager.createNativeQuery(sql.toString());
        q.setParameter("limitPlusOne", size + 1);
        if (hasCountryFilter) q.setParameter("countryName", cn);
        for (int i = 0; i < tokens.size(); i++) {
            q.setParameter("kw" + i, toEscapedContainsPattern(tokens.get(i)));
        }
        bindCursorParams(q, useCursorOnRating, useCursorOnPrice, useCursorOnUpdatedAt,
                lastAvgStar, lastMinPrice, lastUpdatedAt, lastId);

        return toLongIds(q.getResultList());
    }

    // --------- 후보 프리리밋 + 바깥 INSTR (단일 CJK용) ---------
    private List<Long> runInstrTopNForSingle(
            String token, String cn, boolean hasCountryFilter,
            java.time.LocalDateTime lastUpdatedAt, Long lastId,
            Double lastAvgStar, Integer lastMinPrice,
            int size, String key, boolean desc,
            boolean needRating, boolean needMinPrice,
            boolean useCursorOnRating, boolean useCursorOnPrice, boolean useCursorOnUpdatedAt
    ) {
        int preLimit = Math.min(MAX_PRELIMIT, Math.max((size + 1) * 100, MIN_PRELIMIT));

        StringBuilder cand = new StringBuilder();
        cand.append("SELECT b.id FROM product b WHERE b.deleted=false AND b.has_future_stock=1 ");
        if (hasCountryFilter) cand.append("AND b.country_name_cached = :countryName ");
        if (useCursorOnRating) {
            cand.append(" AND (b.avg_review_star_cached ").append(desc ? "<" : ">").append(" :lastM ")
                    .append("  OR (b.avg_review_star_cached = :lastM AND (b.updated_at < :lastUpdatedAt ")
                    .append("      OR (b.updated_at = :lastUpdatedAt AND b.id < :lastId))) )");
        } else if (useCursorOnPrice) {
            cand.append(" AND (b.min_discount_price_cached ").append(desc ? "<" : ">").append(" :lastM ")
                    .append("  OR (b.min_discount_price_cached = :lastM AND (b.updated_at < :lastUpdatedAt ")
                    .append("      OR (b.updated_at = :lastUpdatedAt AND b.id < :lastId))) )");
        } else if (useCursorOnUpdatedAt) {
            cand.append(" AND (b.updated_at ").append(desc ? "<" : ">").append(" :lastUpdatedAt ")
                    .append("  OR (b.updated_at = :lastUpdatedAt AND b.id < :lastId))");
        }
        cand.append(" ORDER BY b.updated_at ").append(desc ? "DESC" : "ASC").append(", b.id DESC ");
        cand.append(" LIMIT :preLimit ");

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT p.id FROM product p ")
                .append("JOIN (").append(cand).append(") s ON s.id = p.id ")
                .append("WHERE p.deleted=false AND p.has_future_stock=1 ");
        if (hasCountryFilter) sql.append("AND p.country_name_cached = :countryName ");
        sql.append(" AND INSTR(p.search_text, :tok) > 0 ");

        appendOrder(sql, key, desc, needRating, needMinPrice);
        sql.append(" LIMIT :limitPlusOne");

        Query q = entityManager.createNativeQuery(sql.toString());
        q.setParameter("tok", token);
        q.setParameter("preLimit", preLimit);
        q.setParameter("limitPlusOne", size + 1);
        if (hasCountryFilter) q.setParameter("countryName", cn);
        bindCursorParams(q, useCursorOnRating, useCursorOnPrice, useCursorOnUpdatedAt,
                lastAvgStar, lastMinPrice, lastUpdatedAt, lastId);

        return toLongIds(q.getResultList());
    }

    private static void appendOrder(StringBuilder sql, String key, boolean desc, boolean needRating, boolean needMinPrice) {
        sql.append(" ");
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
    }

    private static void bindCursorParams(
            Query q,
            boolean useCursorOnRating, boolean useCursorOnPrice, boolean useCursorOnUpdatedAt,
            Double lastAvgStar, Integer lastMinPrice,
            java.time.LocalDateTime lastUpdatedAt, Long lastId
    ) {
        if (useCursorOnRating) {
            q.setParameter("lastM", lastAvgStar);
            q.setParameter("lastUpdatedAt", java.sql.Timestamp.valueOf(lastUpdatedAt));
            q.setParameter("lastId", lastId);
        } else if (useCursorOnPrice) {
            q.setParameter("lastM", lastMinPrice);
            q.setParameter("lastUpdatedAt", java.sql.Timestamp.valueOf(lastUpdatedAt));
            q.setParameter("lastId", lastId);
        } else if (useCursorOnUpdatedAt) {
            q.setParameter("lastUpdatedAt", java.sql.Timestamp.valueOf(lastUpdatedAt));
            q.setParameter("lastId", lastId);
        }
    }

    // ===== 프로브 기반 FTS 시드 선정(DF 테이블 없이 동작) =====
    private List<String> chooseFtsSeedsByProbe(List<String> tokens, boolean hasCountryFilter, String countryName) {
        if (tokens == null || tokens.isEmpty()) return List.of();

        // CJK FTS 허용이 아니면 CJK 토큰은 시드 후보에서 제외
        List<Map.Entry<String,Integer>> ranked = new ArrayList<>();
        for (String t : tokens) {
            if (!USE_CJK_FTS && containsCJK(t)) continue;
            int hits = probeHits(t, hasCountryFilter, countryName);
            ranked.add(new AbstractMap.SimpleEntry<>(t, hits));
        }
        if (ranked.isEmpty()) return List.of();

        // 희소(히트수 적은) 순으로 정렬
        ranked.sort(Comparator.comparingInt(Map.Entry::getValue));

        // 가장 희소한 것부터 최대 MAX_FTS_TOKENS개, 최소추정(=min hits)이 목표 이하가 될 때까지 선택
        int est = Integer.MAX_VALUE;
        List<String> chosen = new ArrayList<>();
        for (Map.Entry<String,Integer> e : ranked) {
            if (chosen.size() >= MAX_FTS_TOKENS) break;
            est = Math.min(est, e.getValue());
            chosen.add(e.getKey());
            if (est <= PROBE_HIT_TARGET) break;
        }

        // 만약 모두 고빈도라면(=est가 여전히 큼) 시드를 비워서 FTS 경로를 타지 않게 한다
        if (chosen.isEmpty() || est > PROBE_HIT_TARGET) return List.of();
        return chosen;
    }

    // 네이티브 결과 안전 캐스팅
    private static List<Long> toLongIds(List<?> rows) {
        if (rows == null || rows.isEmpty()) return List.of();
        List<Long> out = new ArrayList<>(rows.size());
        for (Object o : rows) {
            if (o instanceof Number n) out.add(n.longValue());
            else if (o instanceof Object[] arr && arr.length > 0 && arr[0] instanceof Number n) out.add(n.longValue());
            else if (o != null) {
                try { out.add(Long.parseLong(o.toString())); } catch (Exception ignore) {}
            }
        }
        return out;
    }
}
