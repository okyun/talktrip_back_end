package com.talktrip.talktrip.domain.kafka.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 공통 통계 응답 DTO
 * 
 * 통계 API의 공통 응답 형식입니다.
 * 제네릭 타입을 사용하여 다양한 통계 데이터를 포함할 수 있습니다.
 * 
 * @param <T> 통계 데이터 타입
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StatsResponse<T> {
    
    /**
     * 성공 여부
     */
    private Boolean success;
    
    /**
     * 실제 응답 데이터 (제네릭)
     */
    private T data;
    
    /**
     * 설명 메시지
     */
    private String message;
    
    /**
     * 응답 생성 시간
     * JSON 직렬화 시 "yyyy-MM-dd'T'HH:mm:ss" 형식으로 변환
     */
    @Builder.Default
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp = LocalDateTime.now();
    
    /**
     * 성공 응답 생성
     * 
     * @param data 응답 데이터
     * @param message 메시지
     * @return StatsResponse
     */
    public static <T> StatsResponse<T> success(T data, String message) {
        return StatsResponse.<T>builder()
                .success(true)
                .data(data)
                .message(message)
                .timestamp(LocalDateTime.now())
                .build();
    }
    
    /**
     * 실패 응답 생성
     * 
     * @param message 에러 메시지
     * @return StatsResponse
     */
    public static <T> StatsResponse<T> failure(String message) {
        return StatsResponse.<T>builder()
                .success(false)
                .data(null)
                .message(message)
                .timestamp(LocalDateTime.now())
                .build();
    }
}

