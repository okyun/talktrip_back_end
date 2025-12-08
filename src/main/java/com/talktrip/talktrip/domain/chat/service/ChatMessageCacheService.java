package com.talktrip.talktrip.domain.chat.service;

import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChatMessageCacheService {

    private final StringRedisTemplate stringRedisTemplate;
    private final RedisTemplate<String, Object> redisTemplate;

    // --- key util ---

    private String lastMessageKey(String roomId) {
        // 채팅방별 마지막 메시지 정보
        return "chat:room:" + roomId + ":last";
    }

    private String unreadKey(String roomId, String email) {
        // 채팅방 + 유저별 unreadCount
        return "chat:room:" + roomId + ":unread:" + email;
    }

    // --- lastMessage 캐싱 (HSET) ---

    public void cacheLastMessage(ChatMessage message) {
        try {
            String key = lastMessageKey(message.getRoomId());

            redisTemplate.opsForHash().put(key, "messageId", message.getMessageId());
            redisTemplate.opsForHash().put(key, "roomId", message.getRoomId());
            redisTemplate.opsForHash().put(key, "accountEmail", message.getAccountEmail());
            redisTemplate.opsForHash().put(key, "content", message.getMessage());
            redisTemplate.opsForHash().put(key, "createdAt", message.getCreatedAt().toString());

            // 선택사항: 너무 오래된 방 캐시는 자동 정리하고 싶으면 TTL
            redisTemplate.expire(key, Duration.ofDays(7));

        } catch (Exception e) {
            log.warn("Redis lastMessage 캐싱 실패 - roomId: {}, error: {}",
                    message.getRoomId(), e.getMessage());
        }
    }

    // --- unreadCount 증가 (INCR) ---

    public void incrementUnread(String roomId, String email) {
        try {
            String key = unreadKey(roomId, email);
            stringRedisTemplate.opsForValue().increment(key);
            // 선택: TTL
            stringRedisTemplate.expire(key, Duration.ofDays(7));
        } catch (Exception e) {
            log.warn("Redis unreadCount INCR 실패 - roomId: {}, email: {}, error: {}",
                    roomId, email, e.getMessage());
        }
    }

    // --- 읽음 처리 시 unreadCount 초기화 (0으로 설정 or 삭제) ---

    public void clearUnread(String roomId, String email) {
        try {
            String key = unreadKey(roomId, email);
            // 0으로 초기화
            stringRedisTemplate.opsForValue().set(key, "0");
            // 또는 아예 삭제하는 것도 가능
            // stringRedisTemplate.delete(key);
        } catch (Exception e) {
            log.warn("Redis unreadCount clear 실패 - roomId: {}, email: {}, error: {}",
                    roomId, email, e.getMessage());
        }
    }
    // --- unreadCount 조회 (캐시 우선, 없으면 null) ---
    public Integer getUnread(String roomId, String email) {
        try {
            String key = unreadKey(roomId, email);
            String value = stringRedisTemplate.opsForValue().get(key);
            if (value == null) {
                return null;
            }
            return Integer.parseInt(value);
        } catch (Exception e) {
            log.warn("Redis unreadCount GET 실패 - roomId: {}, email: {}, error: {}",
                    roomId, email, e.getMessage());
            return null; // 실패하면 캐시 포기하고 DB fallback
        }
    }
}
