package com.talktrip.talktrip.domain.kafka.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 기간별 통계 DTO
 * 
 * 특정 기간(윈도우)에 대한 통계 정보를 표현합니다.
 * Kafka Streams에서 윈도우별 집계 결과를 저장할 때 사용됩니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PeriodStats {
    
    /**
     * 윈도우 시작 시간
     */
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime windowStart;
    
    /**
     * 윈도우 종료 시간
     */
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime windowEnd;
    
    /**
     * 해당 기간의 주문 수
     */
    private Long orderCount;
}

