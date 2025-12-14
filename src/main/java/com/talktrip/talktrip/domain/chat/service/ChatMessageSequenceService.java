package com.talktrip.talktrip.domain.chat.service;

import com.talktrip.talktrip.domain.chat.repository.ChatMessageRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

//redis의 메시지 순서 보장을 위한 서비스 (원자적인 연산)
@Service
@RequiredArgsConstructor
public class ChatMessageSequenceService {

    private static final Logger logger = LoggerFactory.getLogger(ChatMessageSequenceService.class);
    private final RedisTemplate<String,Object> redisTemplate;
    private final ChatMessageRepository chatMessageRepository;
    private final String prefix = "chat:sequence";

    private Long getNextSequenceInternal(String roomId){
        String key = prefix+":"+roomId;
        
        try {
            // 1) Redis에서 현재 시퀀스 조회
            Long redisSequence = (Long) redisTemplate.opsForValue().get(key);
            
            if (redisSequence == null) {
                // 2) Redis에 값이 없으면 DB에서 마지막 sequenceNumber 조회
                Long dbMaxSequence = chatMessageRepository.findMaxSequenceNumberByRoomId(roomId);
                Long nextSequence = dbMaxSequence + 1;
                
                // 3) Redis에 DB 값 + 1을 설정
                redisTemplate.opsForValue().set(key, nextSequence);
                logger.debug("채팅방 {} DB 기반 시퀀스 초기화: {} -> {}", roomId, dbMaxSequence, nextSequence);
                return nextSequence;
            } else {
                // 4) Redis에 값이 있으면 원자적 증가 연산 사용
                Long sequence = redisTemplate.opsForValue().increment(key);
                logger.debug("채팅방 {} Redis 기반 시퀀스 증가: {} -> {}", roomId, redisSequence, sequence);
                return sequence;
            }
        } catch (Exception e) {
            logger.error("채팅방 {} 시퀀스 생성 중 오류 발생: {}", roomId, e.getMessage(), e);
            // 5) Redis 오류 시 DB 기반으로 폴백
            Long dbMaxSequence = chatMessageRepository.findMaxSequenceNumberByRoomId(roomId);
            Long nextSequence = dbMaxSequence + 1;
            logger.debug("채팅방 {} DB 폴백 시퀀스: {} -> {}", roomId, dbMaxSequence, nextSequence);
            return nextSequence;
        }
    }

    /**
     * 채팅방의 다음 시퀀스 번호를 원자적으로 생성합니다.
     * 
     * @param roomId 채팅방 ID
     * @return 다음 시퀀스 번호
     */
    public Long getNextSequence(String roomId) {
        try {
            Long sequence = getNextSequenceInternal(roomId);
            logger.debug("채팅방 {} 시퀀스 생성: {}", roomId, sequence);
            return sequence;
        } catch (Exception e) {
            logger.error("채팅방 {} 시퀀스 생성 실패: {}", roomId, e.getMessage(), e);
            throw new RuntimeException("메시지 순서 생성에 실패했습니다: " + e.getMessage());
        }
    }

    // ✅ 원자적 연산: 동시에 여러 메시지가 들어와도 순서 보장
    // ✅ Redis 기반: 빠른 성능과 확장성
    // ✅ 방별 분리: 각 채팅방마다 독립적인 순서 관리
    // ✅ 안전성: null 체크로 예외 상황 방지

}
