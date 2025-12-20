package com.talktrip.talktrip.global.logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ThreadLocal을 사용하여 SQL 파라미터 바인딩 정보를 저장하고 관리
 */
public class SqlParameterBinder {
    
    private static final ThreadLocal<List<ParameterBinding>> PARAMETERS = new ThreadLocal<>();
    
    /**
     * 파라미터 바인딩 정보 저장
     */
    public static void addParameter(int index, String type, String value) {
        List<ParameterBinding> params = PARAMETERS.get();
        if (params == null) {
            params = new ArrayList<>();
            PARAMETERS.set(params);
        }
        params.add(new ParameterBinding(index, type, value));
    }
    
    /**
     * SQL의 ? 플레이스홀더를 실제 파라미터 값으로 치환
     */
    public static String replaceParameters(String sql) {
        List<ParameterBinding> params = PARAMETERS.get();
        if (params == null || params.isEmpty()) {
            // 파라미터가 없으면 원본 SQL 반환
            return sql;
        }
        
        // 인덱스별로 파라미터 맵 생성
        Map<Integer, ParameterBinding> paramMap = new HashMap<>();
        for (ParameterBinding param : params) {
            paramMap.put(param.index, param);
        }
        
        // ? 를 찾아서 실제 값으로 치환 (순서대로)
        Pattern pattern = Pattern.compile("\\?");
        Matcher matcher = pattern.matcher(sql);
        
        StringBuffer sb = new StringBuffer();
        int paramIndex = 1; // JDBC 파라미터는 1부터 시작
        while (matcher.find()) {
            ParameterBinding param = paramMap.get(paramIndex);
            String replacement;
            if (param != null) {
                replacement = formatValue(param.value, param.type);
            } else {
                // 파라미터가 없으면 ? 그대로 유지
                replacement = "?";
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            paramIndex++;
        }
        matcher.appendTail(sb);
        
        return sb.toString();
    }
    
    /**
     * 파라미터 값을 SQL에 적합한 형식으로 포맷팅
     */
    private static String formatValue(String value, String type) {
        if (value == null || "null".equalsIgnoreCase(value)) {
            return "NULL";
        }
        
        // 숫자 타입인지 확인
        if (type != null && (type.contains("INTEGER") || type.contains("BIGINT") || 
                             type.contains("DECIMAL") || type.contains("NUMERIC") ||
                             type.contains("DOUBLE") || type.contains("FLOAT") ||
                             type.contains("LONG"))) {
            return value;
        }
        
        // 날짜/시간 타입
        if (type != null && (type.contains("DATE") || type.contains("TIME") || type.contains("TIMESTAMP"))) {
            // DATE 타입이면 시간 부분 추가
            if (type.contains("DATE") && !value.contains(" ")) {
                return "'" + value + " 00:00:00'";
            }
            return "'" + value + "'";
        }
        
        // 문자열인 경우 따옴표 추가 및 이스케이프
        return "'" + value.replace("'", "''") + "'";
    }
    
    /**
     * 저장된 파라미터 초기화
     */
    public static void clear() {
        PARAMETERS.remove();
    }
    
    /**
     * 파라미터 바인딩 정보
     */
    private static class ParameterBinding {
        int index;
        String type;
        String value;
        
        ParameterBinding(int index, String type, String value) {
            this.index = index;
            this.type = type;
            this.value = value;
        }
    }
}

