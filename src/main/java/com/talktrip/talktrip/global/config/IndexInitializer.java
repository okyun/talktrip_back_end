package com.talktrip.talktrip.global.config;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Component
@RequiredArgsConstructor
@Order(1)
public class IndexInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IndexInitializer.class);

    private final EntityManager entityManager;
    private final DataSource dataSource;

    @Value("${search.index-initializer.enabled:false}")
    private boolean initializerEnabled; // ← 환경변수로 주입

    @Override
    public void run(ApplicationArguments args) {
        if (!initializerEnabled) {
            log.info("[IndexInitializer] Disabled via property 'search.index-initializer.enabled=false'");
            return;
        }

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);

            /* 0) 캐시/검색용 컬럼 보강 */
            addColumnIfNotExists(stmt, "product", "country_name_cached", "VARCHAR(255) NOT NULL DEFAULT '' AFTER country_id");
            addColumnIfNotExists(stmt, "product", "hashtags_cached", "TEXT AFTER deleted_at");
            addColumnIfNotExists(stmt, "product", "search_text", "TEXT AFTER hashtags_cached");

            // 🔥 성능 캐시 컬럼
            addColumnIfNotExists(stmt, "product", "has_future_stock", "TINYINT(1) NOT NULL DEFAULT 0 AFTER search_text");
            addColumnIfNotExists(stmt, "product", "min_discount_price_cached", "INT NULL AFTER has_future_stock");
            addColumnIfNotExists(stmt, "product", "avg_review_star_cached", "DECIMAL(4,2) NULL AFTER min_discount_price_cached");
            addColumnIfNotExists(stmt, "product", "review_count_cached", "INT NULL AFTER avg_review_star_cached");

            /* 1) FULLTEXT (ngram 우선) */
            createFulltextIfNotExists(stmt, "product", "ft_search_text_ko", "search_text");

            /* 2) 인덱스 */
            // seek 계열 (has_future_stock 포함 버전 우선)
            addIndexIfNotExists(stmt, "product", "idx_product_seek_hfs", "(deleted, has_future_stock, updated_at DESC, id DESC)");
            addIndexIfNotExists(stmt, "product", "idx_product_list_seek_hfs", "(deleted, country_name_cached, has_future_stock, updated_at DESC, id DESC)");

            // 구 버전(호환 유지)
            addIndexIfNotExists(stmt, "product", "idx_product_default_seek", "(deleted, updated_at DESC, id DESC)");
            addIndexIfNotExists(stmt, "product", "idx_product_list_seek", "(deleted, country_name_cached, updated_at DESC, id DESC)");

            // 정렬 키 전용
            addIndexIfNotExists(stmt, "product", "idx_product_minprice", "(min_discount_price_cached, updated_at DESC, id DESC)");
            addIndexIfNotExists(stmt, "product", "idx_product_rating", "(avg_review_star_cached, updated_at DESC, id DESC)");

            // 옵션 인덱스 (미래 재고 조회)
            addIndexIfNotExists(stmt, "product_option", "idx_product_option_future", "(product_id, start_date, stock)");

            /* 3) 캐시 백필 */
            safeExec(stmt,
                    "UPDATE product p LEFT JOIN country c ON c.id = p.country_id " +
                            "SET p.country_name_cached = IFNULL(c.name, '') " +
                            "WHERE p.country_name_cached IS NULL OR p.country_name_cached = ''",
                    "[IndexInitializer] Backfilled product.country_name_cached"
            );
            safeExec(stmt,
                    "UPDATE product p LEFT JOIN (SELECT h.product_id, GROUP_CONCAT(h.hashtag SEPARATOR ' ') AS tags FROM hash_tag h GROUP BY h.product_id) t " +
                            "ON t.product_id = p.id SET p.hashtags_cached = IFNULL(t.tags, '')",
                    "[IndexInitializer] Backfilled product.hashtags_cached"
            );
            safeExec(stmt,
                    "UPDATE product p SET p.search_text = TRIM(CONCAT_WS(' ', p.product_name, p.description, p.country_name_cached, IFNULL(p.hashtags_cached, '')))",
                    "[IndexInitializer] Rebuilt product.search_text"
            );

            // has_future_stock / min_discount_price_cached 초기화
            safeExec(stmt,
                    "UPDATE product p " +
                            "LEFT JOIN ( " +
                            "  SELECT po.product_id, " +
                            "         MIN(CASE WHEN po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY THEN po.discount_price END) AS min_dp, " +
                            "         MAX(CASE WHEN po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY THEN 1 ELSE 0 END) AS has_stock " +
                            "  FROM product_option po GROUP BY po.product_id " +
                            ") x ON x.product_id = p.id " +
                            "SET p.has_future_stock = IFNULL(x.has_stock, 0), p.min_discount_price_cached = x.min_dp",
                    "[IndexInitializer] Backfilled has_future_stock / min_discount_price_cached"
            );

            // avg_review_star_cached / review_count_cached 초기화
            safeExec(stmt,
                    "UPDATE product p " +
                            "LEFT JOIN ( " +
                            "  SELECT r.product_id, AVG(r.review_star) AS avg_star, COUNT(*) AS cnt " +
                            "  FROM review r GROUP BY r.product_id " +
                            ") x ON x.product_id = p.id " +
                            "SET p.avg_review_star_cached = IFNULL(x.avg_star, 0.0), p.review_count_cached = IFNULL(x.cnt, 0)",
                    "[IndexInitializer] Backfilled avg_review_star_cached / review_count_cached"
            );

            /* 4) 트리거 생성 (검색 캐시 유지 + 옵션/리뷰 캐시 유지) */

            // product: BEFORE INSERT/UPDATE → 검색 캐시
            createTrigger(stmt, "bi_product_search_cache",
                    "CREATE TRIGGER bi_product_search_cache BEFORE INSERT ON product FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_country_name VARCHAR(255) DEFAULT ''; " +
                            "IF NEW.country_id IS NOT NULL THEN SELECT name INTO v_country_name FROM country WHERE id = NEW.country_id; END IF; " +
                            "SET NEW.country_name_cached = IFNULL(v_country_name, ''); " +
                            "SET NEW.search_text = TRIM(CONCAT_WS(' ', NEW.product_name, NEW.description, NEW.country_name_cached, IFNULL(NEW.hashtags_cached, ''))); " +
                            "END");

            createTrigger(stmt, "bu_product_search_cache",
                    "CREATE TRIGGER bu_product_search_cache BEFORE UPDATE ON product FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_country_name VARCHAR(255) DEFAULT ''; " +
                            "IF NEW.country_id IS NOT NULL THEN SELECT name INTO v_country_name FROM country WHERE id = NEW.country_id; END IF; " +
                            "SET NEW.country_name_cached = IFNULL(v_country_name, ''); " +
                            "SET NEW.search_text = TRIM(CONCAT_WS(' ', NEW.product_name, NEW.description, NEW.country_name_cached, IFNULL(NEW.hashtags_cached, ''))); " +
                            "END");

            // hash_tag: AFTER I/U/D → hashtags_cached + search_text 재구축
            createTrigger(stmt, "ai_hash_tag_rebuild_product_cache",
                    "CREATE TRIGGER ai_hash_tag_rebuild_product_cache AFTER INSERT ON hash_tag FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_tags TEXT DEFAULT ''; " +
                            "SELECT GROUP_CONCAT(h.hashtag SEPARATOR ' ') INTO v_tags FROM hash_tag h WHERE h.product_id = NEW.product_id; " +
                            "UPDATE product p SET p.hashtags_cached = IFNULL(v_tags, ''), " +
                            "p.search_text = TRIM(CONCAT_WS(' ', p.product_name, p.description, p.country_name_cached, IFNULL(v_tags, ''))) " +
                            "WHERE p.id = NEW.product_id; " +
                            "END");

            createTrigger(stmt, "au_hash_tag_rebuild_product_cache",
                    "CREATE TRIGGER au_hash_tag_rebuild_product_cache AFTER UPDATE ON hash_tag FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_pid BIGINT; DECLARE v_tags TEXT DEFAULT ''; " +
                            "SET v_pid = NEW.product_id; " +
                            "SELECT GROUP_CONCAT(h.hashtag SEPARATOR ' ') INTO v_tags FROM hash_tag h WHERE h.product_id = v_pid; " +
                            "UPDATE product p SET p.hashtags_cached = IFNULL(v_tags, ''), " +
                            "p.search_text = TRIM(CONCAT_WS(' ', p.product_name, p.description, p.country_name_cached, IFNULL(v_tags, ''))) " +
                            "WHERE p.id = v_pid; " +
                            "END");

            createTrigger(stmt, "ad_hash_tag_rebuild_product_cache",
                    "CREATE TRIGGER ad_hash_tag_rebuild_product_cache AFTER DELETE ON hash_tag FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_tags TEXT DEFAULT ''; " +
                            "SELECT GROUP_CONCAT(h.hashtag SEPARATOR ' ') INTO v_tags FROM hash_tag h WHERE h.product_id = OLD.product_id; " +
                            "UPDATE product p SET p.hashtags_cached = IFNULL(v_tags, ''), " +
                            "p.search_text = TRIM(CONCAT_WS(' ', p.product_name, p.description, p.country_name_cached, IFNULL(v_tags, ''))) " +
                            "WHERE p.id = OLD.product_id; " +
                            "END");

            // product_option: AFTER I/U/D → has_future_stock / min_discount_price_cached 유지
            createTrigger(stmt, "ai_product_option_refresh_product_cache",
                    "CREATE TRIGGER ai_product_option_refresh_product_cache AFTER INSERT ON product_option FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_min INT; DECLARE v_has_stock TINYINT(1); " +
                            "SELECT MIN(po.discount_price) INTO v_min FROM product_option po WHERE po.product_id = NEW.product_id AND po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY; " +
                            "SELECT IF(COUNT(*) > 0, 1, 0) INTO v_has_stock FROM product_option po WHERE po.product_id = NEW.product_id AND po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY; " +
                            "UPDATE product p SET p.min_discount_price_cached = v_min, p.has_future_stock = IFNULL(v_has_stock,0) WHERE p.id = NEW.product_id; " +
                            "END");

            createTrigger(stmt, "au_product_option_refresh_product_cache",
                    "CREATE TRIGGER au_product_option_refresh_product_cache AFTER UPDATE ON product_option FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_pid BIGINT; DECLARE v_min INT; DECLARE v_has_stock TINYINT(1); " +
                            "SET v_pid = NEW.product_id; " +
                            "SELECT MIN(po.discount_price) INTO v_min FROM product_option po WHERE po.product_id = v_pid AND po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY; " +
                            "SELECT IF(COUNT(*) > 0, 1, 0) INTO v_has_stock FROM product_option po WHERE po.product_id = v_pid AND po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY; " +
                            "UPDATE product p SET p.min_discount_price_cached = v_min, p.has_future_stock = IFNULL(v_has_stock,0) WHERE p.id = v_pid; " +
                            "END");

            createTrigger(stmt, "ad_product_option_refresh_product_cache",
                    "CREATE TRIGGER ad_product_option_refresh_product_cache AFTER DELETE ON product_option FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_min INT; DECLARE v_has_stock TINYINT(1); " +
                            "SELECT MIN(po.discount_price) INTO v_min FROM product_option po WHERE po.product_id = OLD.product_id AND po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY; " +
                            "SELECT IF(COUNT(*) > 0, 1, 0) INTO v_has_stock FROM product_option po WHERE po.product_id = OLD.product_id AND po.stock > 0 AND po.start_date >= CURDATE() + INTERVAL 1 DAY; " +
                            "UPDATE product p SET p.min_discount_price_cached = v_min, p.has_future_stock = IFNULL(v_has_stock,0) WHERE p.id = OLD.product_id; " +
                            "END");

            // review: AFTER I/U/D → avg_review_star_cached / review_count_cached 유지
            createTrigger(stmt, "ai_review_refresh_product_cache",
                    "CREATE TRIGGER ai_review_refresh_product_cache AFTER INSERT ON review FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_avg DECIMAL(4,2); DECLARE v_cnt INT; " +
                            "SELECT AVG(r.review_star), COUNT(*) INTO v_avg, v_cnt FROM review r WHERE r.product_id = NEW.product_id; " +
                            "UPDATE product p SET p.avg_review_star_cached = IFNULL(v_avg,0.0), p.review_count_cached = IFNULL(v_cnt,0) WHERE p.id = NEW.product_id; " +
                            "END");

            createTrigger(stmt, "au_review_refresh_product_cache",
                    "CREATE TRIGGER au_review_refresh_product_cache AFTER UPDATE ON review FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_pid BIGINT; DECLARE v_avg DECIMAL(4,2); DECLARE v_cnt INT; " +
                            "SET v_pid = NEW.product_id; " +
                            "SELECT AVG(r.review_star), COUNT(*) INTO v_avg, v_cnt FROM review r WHERE r.product_id = v_pid; " +
                            "UPDATE product p SET p.avg_review_star_cached = IFNULL(v_avg,0.0), p.review_count_cached = IFNULL(v_cnt,0) WHERE p.id = v_pid; " +
                            "END");

            createTrigger(stmt, "ad_review_refresh_product_cache",
                    "CREATE TRIGGER ad_review_refresh_product_cache AFTER DELETE ON review FOR EACH ROW " +
                            "BEGIN " +
                            "DECLARE v_avg DECIMAL(4,2); DECLARE v_cnt INT; " +
                            "SELECT AVG(r.review_star), COUNT(*) INTO v_avg, v_cnt FROM review r WHERE r.product_id = OLD.product_id; " +
                            "UPDATE product p SET p.avg_review_star_cached = IFNULL(v_avg,0.0), p.review_count_cached = IFNULL(v_cnt,0) WHERE p.id = OLD.product_id; " +
                            "END");

        } catch (Exception e) {
            log.warn("[IndexInitializer] Initialization encountered issues: {}", e.getMessage());
        }
    }

    /* ===== helper ===== */

    private void addColumnIfNotExists(Statement stmt, String table, String column, String ddlTail) {
        try {
            if (!columnExists(table, column)) {
                stmt.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + ddlTail);
                log.info("[IndexInitializer] Added column {}.{}", table, column);
            }
        } catch (Exception e) {
            log.debug("[IndexInitializer] {}.{} add skipped: {}", table, column, e.getMessage());
        }
    }

    private void addIndexIfNotExists(Statement stmt, String table, String index, String cols) {
        try {
            if (!indexExists(table, index, false)) {
                stmt.execute("ALTER TABLE " + table + " ADD INDEX " + index + " " + cols);
                log.info("[IndexInitializer] Created index {} on {}", index, table);
            }
        } catch (Exception e) {
            log.debug("[IndexInitializer] index {} skip: {}", index, e.getMessage());
        }
    }

    private void createFulltextIfNotExists(Statement stmt, String table, String index, String column) {
        try {
            if (!indexExists(table, index, true)) {
                try {
                    stmt.execute("ALTER TABLE " + table + " ADD FULLTEXT " + index + " (" + column + ") WITH PARSER ngram");
                    log.info("[IndexInitializer] Created FULLTEXT {} (ngram)", index);
                } catch (Exception e) {
                    stmt.execute("ALTER TABLE " + table + " ADD FULLTEXT " + index + " (" + column + ")");
                    log.info("[IndexInitializer] Created FULLTEXT {} (default parser)", index);
                }
            }
        } catch (Exception e) {
            log.debug("[IndexInitializer] FULLTEXT {} skip: {}", index, e.getMessage());
        }
    }

    private void createTrigger(Statement stmt, String name, String ddl) {
        try {
            stmt.execute("DROP TRIGGER IF EXISTS " + name);
            stmt.execute(ddl);
            log.info("[IndexInitializer] Created trigger {}", name);
        } catch (Exception e) {
            log.debug("[IndexInitializer] trigger {} skipped: {}", name, e.getMessage());
        }
    }

    private void safeExec(Statement stmt, String sql, String okMsg) {
        try {
            stmt.execute(sql);
            log.info(okMsg);
        } catch (Exception e) {
            log.debug("{} skipped: {}", okMsg, e.getMessage());
        }
    }

    private boolean indexExists(String table, String indexName, boolean fulltext) {
        try {
            String sql = fulltext
                    ? "SELECT COUNT(1) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_type = 'FULLTEXT' AND index_name = ?"
                    : "SELECT COUNT(1) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?";
            Query q = entityManager.createNativeQuery(sql);
            q.setParameter(1, table);
            q.setParameter(2, indexName);
            Number n = (Number) q.getSingleResult();
            return n != null && n.intValue() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean columnExists(String table, String column) {
        try {
            Query q = entityManager.createNativeQuery(
                    "SELECT COUNT(1) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?"
            );
            q.setParameter(1, table);
            q.setParameter(2, column);
            Number n = (Number) q.getSingleResult();
            return n != null && n.intValue() > 0;
        } catch (Exception e) {
            return false;
        }
    }
}
