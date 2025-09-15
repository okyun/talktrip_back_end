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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;

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

    // ===== 희귀 토큰 기반 FTS 프리필터링 설정 =====
    // 총 문서 수 대략치(운영에선 캐시/환경변수/테이블 카운트 캐싱 권장)
    private static final int TOTAL_DOCS_APPROX = 10_000_000;
    // 희귀 기준: 전체의 1.5% 또는 절대 150k 이하
    private static final double RARE_RATIO = 0.015;
    private static final int RARE_ABS_FLOOR = 150_000;
    // FTS에 넣을 희귀 토큰 최대 개수 (너무 많으면 오히려 느려짐)
    private static final int MAX_FTS_TOKENS = 4;
    // 후보군 목표 상한(DF 추정으로 이 값 밑으로 떨어질 때까지 희귀 토큰 추가)
    private static final int CAND_TARGET = 250_000;

    // ---------- token utils ----------
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
                .limit(5) // 과도한 키워드 남용 방지
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

    // **CJK면 무조건 FTS 비선호** 규칙을 완화:
    // → DF가 낮으면(CJK라도) FTS 후보축소에 적극 활용
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

        // 커서 적용 여부(모두 채워졌을 때만 WHERE에 추가)
        boolean useCursorOnRating    = needRating && lastAvgStar != null && lastUpdatedAt != null && lastId != null;
        boolean useCursorOnPrice     = needMinPrice && lastMinPrice != null && lastUpdatedAt != null && lastId != null;
        boolean useCursorOnUpdatedAt = "updatedAt".equals(key) && lastUpdatedAt != null && lastId != null;

        // 키워드 없음
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
        // 1) DF 로드 & 희귀 토큰 선택
        Map<String, Integer> dfMap = loadDfMap(tokens);
        List<String> rareTokens = pickRareTokens(tokens, dfMap);
        List<String> ftsTokens = chooseFtsTokensByTarget(rareTokens, dfMap, CAND_TARGET, MAX_FTS_TOKENS);

        if (!ftsTokens.isEmpty()) {
            // (A) 희귀 토큰들로 FTS 후보 축소 → 최종 AND는 INSTR로 모든 토큰 검증
            StringBuilder cand = new StringBuilder();
            cand.append("SELECT b.id FROM product b WHERE b.deleted=false AND b.has_future_stock=1 ");
            if (hasCountryFilter) cand.append("AND b.country_name_cached = :countryName ");
            cand.append("AND MATCH(b.search_text) AGAINST (:ftq IN BOOLEAN MODE) ");
            // 커서 조건(후보에도 동일 적용해 스캔량 감소; 정합성 OK)
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
            // 모든 토큰 검증(INSTR: 와일드카드/이스케이프 불필요, CJK에도 효율적)
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

        // (B) DF 미가용/희귀 토큰 부재 → 기존 안정 경로: LIKE AND
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

    // --------- 후보 프리리밋 + 바깥 INSTR/LIKE (단일 CJK용) ---------
    private List<Long> runInstrTopNForSingle(
            String token, String cn, boolean hasCountryFilter,
            java.time.LocalDateTime lastUpdatedAt, Long lastId,
            Double lastAvgStar, Integer lastMinPrice,
            int size, String key, boolean desc,
            boolean needRating, boolean needMinPrice,
            boolean useCursorOnRating, boolean useCursorOnPrice, boolean useCursorOnUpdatedAt
    ) {
        int preLimit = Math.min(MAX_PRELIMIT, Math.max((size + 1) * 100, MIN_PRELIMIT));

        // 1) 후보 집합: 정렬 인덱스(updated_at, id)로 상위 N개만 확보 (+ 동일 커서 조건 포함)
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

        // 2) 본문: 후보와 조인 후 INSTR/LIKE 필터링
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

    // ===== DF 로드 & 희귀 토큰 선택 =====
    private Map<String, Integer> loadDfMap(List<String> tokens) {
        if (tokens == null || tokens.isEmpty()) return Map.of();
        try {
            StringBuilder sql = new StringBuilder("SELECT token, df FROM product_ft_df WHERE token IN (");
            for (int i = 0; i < tokens.size(); i++) {
                if (i > 0) sql.append(",");
                sql.append(":t").append(i);
            }
            sql.append(")");
            Query q = entityManager.createNativeQuery(sql.toString());
            for (int i = 0; i < tokens.size(); i++) q.setParameter("t" + i, tokens.get(i));
            List<?> rows = q.getResultList();
            Map<String, Integer> map = new HashMap<>();
            for (Object row : rows) {
                if (row instanceof Object[] arr && arr.length >= 2) {
                    String tok = String.valueOf(arr[0]);
                    Integer df = ((Number) arr[1]).intValue();
                    map.put(tok, df);
                }
            }
            return map;
        } catch (Exception e) {
            // 테이블 없거나 권한 이슈 → 빈 맵(폴백 경로 사용)
            return Map.of();
        }
    }

    private List<String> pickRareTokens(List<String> tokens, Map<String, Integer> dfMap) {
        if (tokens.isEmpty() || dfMap.isEmpty()) return List.of();
        int rareThreshold = Math.max(RARE_ABS_FLOOR, (int) Math.round(TOTAL_DOCS_APPROX * RARE_RATIO));
        List<String> rares = new ArrayList<>();
        for (String t : tokens) {
            Integer df = dfMap.get(t);
            if (df != null && df <= rareThreshold) rares.add(t);
        }
        // DF 오름차순으로 정렬(더 희귀한 것부터)
        rares.sort(Comparator.comparingInt(dfMap::get));
        return rares;
    }

    private List<String> chooseFtsTokensByTarget(
            List<String> rareTokens, Map<String, Integer> dfMap, int target, int maxTokens
    ) {
        if (rareTokens.isEmpty()) return List.of();
        long est = TOTAL_DOCS_APPROX;
        List<String> chosen = new ArrayList<>();
        for (String tok : rareTokens) {
            if (chosen.size() >= maxTokens) break;
            Integer df = dfMap.get(tok);
            if (df == null) continue;
            // BOOLEAN AND 교집합의 상한은 min(df)로 수렴한다고 가정하여 간단히 사용
            est = Math.min(est, df.longValue());
            chosen.add(tok);
            if (est <= target) break;
        }
        return chosen;
    }

    // 네이티브 결과 안전 캐스팅(hibernate가 BigInteger 등으로 반환해도 대응)
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
