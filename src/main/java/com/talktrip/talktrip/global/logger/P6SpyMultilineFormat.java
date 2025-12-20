package com.talktrip.talktrip.global.logger;

import com.p6spy.engine.spy.appender.MessageFormattingStrategy;

public class P6SpyMultilineFormat implements MessageFormattingStrategy {
    
    @Override
    public String formatMessage(int connectionId, String now, long elapsed, String category, String prepared, String sql, String url) {
        // prepared SQL이 있으면 사용 (파라미터가 ?로 표시된 SQL)
        String sqlToFormat = null;
        if (prepared != null && !prepared.isBlank()) {
            sqlToFormat = prepared;
        } else if (sql != null && !sql.isBlank()) {
            sqlToFormat = sql;
        }
        
        if (sqlToFormat == null || sqlToFormat.isBlank()) {
            return "";
        }
        
        // JPQL 주석 부분 제거하고 실제 SQL만 추출
        sqlToFormat = removeJpqlComment(sqlToFormat);
        
        // 파라미터가 ?로 표시된 SQL이면 실제 값으로 치환
        // category가 "statement"이고 파라미터가 있는 경우에만 치환 시도
        if ("statement".equals(category) && sqlToFormat.contains("?")) {
            // 파라미터 바인딩 정보가 ThreadLocal에 저장되어 있을 때 치환
            String replaced = SqlParameterBinder.replaceParameters(sqlToFormat);
            // 치환이 실제로 이루어졌는지 확인 (?가 사라졌는지 확인)
            if (replaced != null && !replaced.equals(sqlToFormat) && !replaced.contains("?")) {
                sqlToFormat = replaced;
                // 사용 후 정리 (다음 쿼리를 위해)
                SqlParameterBinder.clear();
            }
        }
        
        String pretty = prettySql(sqlToFormat);
        return new StringBuilder()
                .append("/* time=").append(elapsed).append("ms, category=").append(category)
                .append(", connection=").append(connectionId).append(" */\n")
                .append(pretty)
                .toString();
    }
    
    /**
     * SQL에서 JPQL 주석 부분 제거
     */
    private String removeJpqlComment(String sql) {
        if (sql == null || sql.isBlank()) {
            return sql;
        }
        
        // /* 로 시작하는 주석 찾기
        int commentStart = sql.indexOf("/*");
        if (commentStart == -1) {
            return sql; // 주석이 없으면 그대로 반환
        }
        
        // */ 로 끝나는 주석 찾기
        int commentEnd = sql.indexOf("*/", commentStart);
        if (commentEnd == -1) {
            return sql; // 주석이 닫히지 않았으면 그대로 반환
        }
        
        // 주석 부분 제거
        String beforeComment = sql.substring(0, commentStart).trim();
        String afterComment = sql.substring(commentEnd + 2).trim();
        
        // 주석 앞뒤의 공백 제거 후 결합
        return (beforeComment + " " + afterComment).trim();
    }
    
    /**
     * SQL을 보기 좋게 포맷팅
     */
    private String prettySql(String sql) {
        // Normalize whitespace first
        String normalized = sql.replaceAll("\\s+", " ").trim();
        
        // Insert line breaks before common SQL keywords to make it multi-line
        String[] keywords = {
                "select", "from", "where", "group by", "having", "order by", "limit", "offset",
                "inner join", "left join", "right join", "join", "on", "and", "or", "values", "set",
                "insert into", "update", "delete from", "exists"
        };
        
        String pretty = normalized;
        for (String kw : keywords) {
            // case-insensitive replace: add line break before keyword, but avoid double breaks
            pretty = pretty.replaceAll("(?i)\\s+" + kw + "\\s", "\n    " + kw.toUpperCase() + " ");
        }
        
        // Ensure the very first SELECT (or other starting keyword) is on its own line without indentation
        pretty = pretty.replaceFirst("^(?i)SELECT", "SELECT");
        pretty = pretty.replaceFirst("^(?i)INSERT", "INSERT");
        pretty = pretty.replaceFirst("^(?i)UPDATE", "UPDATE");
        pretty = pretty.replaceFirst("^(?i)DELETE", "DELETE");
        
        // Clean up multiple newlines
        pretty = pretty.replaceAll("\n{2,}", "\n");
        
        return pretty;
    }
}

