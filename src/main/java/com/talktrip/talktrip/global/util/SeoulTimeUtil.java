package com.talktrip.talktrip.global.util;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 서울 시간대(Asia/Seoul) 기준으로 시간을 관리하는 유틸리티 클래스
 * 
 * 모든 시간 관련 처리를 서울 시간대로 통일하여 관리
 * - 데이터베이스 저장 시 서울 시간대 사용
 * - 로그 출력 시 서울 시간대 사용
 * - API 응답 시 서울 시간대 사용
 */
public class SeoulTimeUtil {
    
    /**
     * 서울 시간대 ZoneId 상수
     */
    public static final ZoneId SEOUL_ZONE = ZoneId.of("Asia/Seoul");
    
    /**
     * 서울 시간대 기준 현재 시간을 LocalDateTime으로 반환
     * 
     * @return 서울 시간대 기준 현재 LocalDateTime
     */
    public static LocalDateTime now() {
        return LocalDateTime.now(SEOUL_ZONE);
    }
    
    /**
     * 서울 시간대 기준 현재 시간을 Timestamp로 반환
     * 
     * @return 서울 시간대 기준 현재 Timestamp
     */
    public static Timestamp nowAsTimestamp() {
        return Timestamp.valueOf(now());
    }
    
    /**
     * 서울 시간대 기준 현재 시간을 문자열로 반환
     * 
     * @return 서울 시간대 기준 현재 시간 문자열 (yyyy-MM-dd HH:mm:ss)
     */
    public static String nowAsString() {
        return now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    
    /**
     * 서울 시간대 기준 현재 시간을 지정된 포맷의 문자열로 반환
     * 
     * @param pattern 날짜/시간 포맷 패턴
     * @return 서울 시간대 기준 현재 시간 문자열
     */
    public static String nowAsString(String pattern) {
        return now().format(DateTimeFormatter.ofPattern(pattern));
    }
    
    /**
     * UTC 시간을 서울 시간대로 변환
     * 
     * @param utcDateTime UTC 시간
     * @return 서울 시간대로 변환된 LocalDateTime
     */
    public static LocalDateTime convertFromUtc(LocalDateTime utcDateTime) {
        return utcDateTime.atZone(ZoneId.of("UTC"))
                         .withZoneSameInstant(SEOUL_ZONE)
                         .toLocalDateTime();
    }
    
    /**
     * 서울 시간을 UTC로 변환
     * 
     * @param seoulDateTime 서울 시간
     * @return UTC로 변환된 LocalDateTime
     */
    public static LocalDateTime convertToUtc(LocalDateTime seoulDateTime) {
        return seoulDateTime.atZone(SEOUL_ZONE)
                           .withZoneSameInstant(ZoneId.of("UTC"))
                           .toLocalDateTime();
    }
    
    /**
     * MySQL의 CONVERT_TZ 함수를 사용한 서울 시간대 변환 SQL
     * 
     * @return CONVERT_TZ(NOW(), '+00:00', '+09:00') SQL 문자열
     */
    public static String getConvertTzSql() {
        return "CONVERT_TZ(NOW(), '+00:00', '+09:00')";
    }
    
    /**
     * 서울 시간대 기준으로 특정 시간만큼 더한 시간 반환
     * 
     * @param hours 더할 시간 (시간)
     * @return 서울 시간대 기준 현재 시간 + 지정된 시간
     */
    public static LocalDateTime plusHours(long hours) {
        return now().plusHours(hours);
    }
    
    /**
     * 서울 시간대 기준으로 특정 분만큼 더한 시간 반환
     * 
     * @param minutes 더할 분 (분)
     * @return 서울 시간대 기준 현재 시간 + 지정된 분
     */
    public static LocalDateTime plusMinutes(long minutes) {
        return now().plusMinutes(minutes);
    }
    
    /**
     * 서울 시간대 기준으로 특정 일만큼 더한 시간 반환
     * 
     * @param days 더할 일수
     * @return 서울 시간대 기준 현재 시간 + 지정된 일수
     */
    public static LocalDateTime plusDays(long days) {
        return now().plusDays(days);
    }
}
