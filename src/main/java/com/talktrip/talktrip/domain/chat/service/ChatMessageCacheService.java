package com.talktrip.talktrip.domain.chat.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.talktrip.talktrip.domain.chat.dto.response.ChatMemberRoomWithMessageDto;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDTO;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import com.talktrip.talktrip.domain.chat.enums.RoomType;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

/**
 * 채팅 Redis 캐시 서비스.
 * <p>[Redis 캐시 사용 범위]
 * <ul>
 *   <li>Unread: chat:room:{roomId}:unread:{email}, chat:user:{email}:totalUnread (String, INCR/DECR)</li>
 *   <li>Last message: chat:room:{roomId}:last (Hash)</li>
 *   <li>최근 메시지: chat:room:{roomId}:recent (List, 최대 50개)</li>
 *   <li>채팅방 목록: chat:user:{email}:rooms (ZSET), chat:room:{roomId}:meta (Hash)</li>
 * </ul>
 * TTL 7일 공통.
 */
@Service
@RequiredArgsConstructor
public class ChatMessageCacheService {

    private static final Logger logger = LoggerFactory.getLogger(ChatMessageCacheService.class);
    private static final int RECENT_MESSAGE_MAX = 50;
    private static final Duration TTL_DAYS = Duration.ofDays(7);

    private final StringRedisTemplate stringRedisTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    // --- Redis key 이름 정의 (캐시 키 prefix/패턴) ---

    private String lastMessageKey(String roomId) {
        return "chat:room:" + roomId + ":last";
    }

    private String unreadKey(String roomId, String email) {
        return "chat:room:" + roomId + ":unread:" + email;
    }

    private String totalUnreadKey(String email) {
        return "chat:user:" + email + ":totalUnread";
    }

    private String userRoomsKey(String email) {
        return "chat:user:" + email + ":rooms";
    }

    private String roomMetaKey(String roomId) {
        return "chat:room:" + roomId + ":meta";
    }

    private String recentKey(String roomId) {
        return "chat:room:" + roomId + ":recent";
    }

    // ========== 1) Unread 읽기/쓰기 ==========
    // [Redis 캐시] chat:room:{roomId}:last (Hash), chat:room:{roomId}:unread:{email} (String), chat:user:{email}:totalUnread (String)

    /** [Redis 캐시] 방별 마지막 메시지 저장 — Hash, TTL 7일 */
    public void cacheLastMessage(ChatMessage message, String sender) {
        try {
            String key = lastMessageKey(message.getRoomId());
            redisTemplate.opsForHash().put(key, "messageId", message.getMessageId());   // Redis: Hash
            redisTemplate.opsForHash().put(key, "roomId", message.getRoomId());
            redisTemplate.opsForHash().put(key, "sender", sender);
            redisTemplate.opsForHash().put(key, "content", message.getMessage());
            redisTemplate.opsForHash().put(key, "createdAt", message.getCreatedAt().toString());
            redisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis lastMessage 캐싱 실패 - roomId: {}, error: {}",
                    message.getRoomId(), e.getMessage());
        }
    }

    /** [Redis 캐시] 방별 읽지 않음 +1 (INCR), totalUnread도 INCR */
    public void incrementUnread(String roomId, String email) {
        try {
            String key = unreadKey(roomId, email);
            stringRedisTemplate.opsForValue().increment(key);  // Redis: INCR
            stringRedisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
            incrementTotalUnread(email);
        } catch (Exception e) {
            logger.warn("Redis unreadCount INCR 실패 - roomId: {}, email: {}, error: {}",
                    roomId, email, e.getMessage());
        }
    }

    /** [Redis 캐시] 방별 unread 0으로 초기화, totalUnread에서 차감 */
    public void clearUnread(String roomId, String email) {
        try {
            String key = unreadKey(roomId, email);
            String current = stringRedisTemplate.opsForValue().get(key);  // Redis: GET
            int delta = 0;
            if (current != null && !current.isEmpty()) {
                try {
                    delta = Integer.parseInt(current);
                } catch (NumberFormatException ignored) {}
            }
            stringRedisTemplate.opsForValue().set(key, "0");  // Redis: SET
            stringRedisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
            if (delta > 0) {
                decrementTotalUnread(email, delta);
            }
        } catch (Exception e) {
            logger.warn("Redis unreadCount clear 실패 - roomId: {}, email: {}, error: {}",
                    roomId, email, e.getMessage());
        }
    }

    /** [Redis 캐시] 전체 읽지 않음 수 INCR (내부용) */
    private void incrementTotalUnread(String email) {
        try {
            stringRedisTemplate.opsForValue().increment(totalUnreadKey(email));  // Redis: INCR
            stringRedisTemplate.expire(totalUnreadKey(email), TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis totalUnread INCR 실패 - email: {}", email, e);
        }
    }

    /** [Redis 캐시] 전체 읽지 않음 수 DECR (내부용) */
    private void decrementTotalUnread(String email, int delta) {
        try {
            String key = totalUnreadKey(email);
            Long v = stringRedisTemplate.opsForValue().decrement(key, delta);  // Redis: DECR
            if (v != null && v < 0) {
                stringRedisTemplate.opsForValue().set(key, "0");  // Redis: 보정
            }
            stringRedisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis totalUnread DECR 실패 - email: {}", email, e);
        }
    }

    /** [Redis 캐시] 방별 unread 개수 조회 (없으면 empty) */
    public Optional<Integer> getUnread(String roomId, String email) {
        try {
            String key = unreadKey(roomId, email);
            String v = stringRedisTemplate.opsForValue().get(key);  // Redis: GET
            if (v == null || v.isEmpty()) return Optional.empty();
            return Optional.of(Integer.parseInt(v));
        } catch (Exception e) {
            logger.debug("Redis getUnread 실패 - roomId: {}, email: {}", roomId, email, e);
            return Optional.empty();
        }
    }

    /** [Redis 캐시] 전체 unread 개수 조회 (뱃지용, 없으면 empty) */
    public Optional<Integer> getTotalUnread(String email) {
        try {
            String v = stringRedisTemplate.opsForValue().get(totalUnreadKey(email));  // Redis: GET
            if (v == null || v.isEmpty()) return Optional.empty();
            return Optional.of(Integer.parseInt(v));
        } catch (Exception e) {
            logger.debug("Redis getTotalUnread 실패 - email: {}", email, e);
            return Optional.empty();
        }
    }

    /** [Redis 캐시] 캐시 워밍: DB 값으로 totalUnread 설정 */
    public void setTotalUnread(String email, int count) {
        try {
            String key = totalUnreadKey(email);
            stringRedisTemplate.opsForValue().set(key, String.valueOf(count));  // Redis: SET
            stringRedisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis setTotalUnread 실패 - email: {}", email, e);
        }
    }

    // ========== 2) 최근 메시지 N개 List ==========
    // [Redis 캐시] chat:room:{roomId}:recent (List, 최대 50개, TTL 7일)

    /** [Redis 캐시] 최근 메시지 List에 LPUSH + LTRIM(0, 49) */
    public void pushRecentMessage(String roomId, ChatMemberRoomWithMessageDto dto) {
        try {
            String key = recentKey(roomId);
            String json = objectMapper.writeValueAsString(dto);
            stringRedisTemplate.opsForList().leftPush(key, json);  // Redis: LPUSH
            stringRedisTemplate.opsForList().trim(key, 0, RECENT_MESSAGE_MAX - 1);  // Redis: LTRIM
            stringRedisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
        } catch (JsonProcessingException e) {
            logger.warn("Redis pushRecentMessage 직렬화 실패 - roomId: {}", roomId, e);
        } catch (Exception e) {
            logger.warn("Redis pushRecentMessage 실패 - roomId: {}", roomId, e);
        }
    }

    /** [Redis 캐시] 최근 메시지 최대 limit개 조회 (LRANGE, 최신 순). 실패 시 빈 리스트 */
    public List<ChatMemberRoomWithMessageDto> getRecentMessages(String roomId, int limit) {
        try {
            String key = recentKey(roomId);
            List<String> jsons = stringRedisTemplate.opsForList().range(key, 0, limit - 1);  // Redis: LRANGE
            if (jsons == null || jsons.isEmpty()) return List.of();
            List<ChatMemberRoomWithMessageDto> result = new ArrayList<>();
            for (String j : jsons) {
                try {
                    result.add(objectMapper.readValue(j, ChatMemberRoomWithMessageDto.class));
                } catch (JsonProcessingException e) {
                    logger.debug("Redis recent 메시지 역직렬화 실패: {}", j);
                }
            }
            return result;
        } catch (Exception e) {
            logger.debug("Redis getRecentMessages 실패 - roomId: {}", roomId, e);
            return List.of();
        }
    }

    // ========== 3) user:rooms ZSET + room:meta ==========
    // [Redis 캐시] chat:user:{email}:rooms (ZSET, score=updatedAtMs), chat:room:{roomId}:meta (Hash)

    /** [Redis 캐시] 사용자별 채팅방 ID ZADD (score=updatedAtMs) */
    public void addUserRoom(String email, String roomId, long updatedAtMs) {
        try {
            stringRedisTemplate.opsForZSet().add(userRoomsKey(email), roomId, updatedAtMs);  // Redis: ZADD
            stringRedisTemplate.expire(userRoomsKey(email), TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis addUserRoom 실패 - email: {}, roomId: {}", email, roomId, e);
        }
    }

    /** [Redis 캐시] 사용자별 채팅방에서 roomId ZREM */
    public void removeUserRoom(String email, String roomId) {
        try {
            stringRedisTemplate.opsForZSet().remove(userRoomsKey(email), roomId);  // Redis: ZREM
        } catch (Exception e) {
            logger.warn("Redis removeUserRoom 실패 - email: {}, roomId: {}", email, roomId, e);
        }
    }

    /** [Redis 캐시] 방 메타 저장 (Hash, 생성/워밍 시) */
    public void setRoomMeta(String roomId, LocalDateTime updatedAt, String lastMessage, String title, RoomType roomType) {
        try {
            String key = roomMetaKey(roomId);
            redisTemplate.opsForHash().put(key, "updatedAt", updatedAt.toString());  // Redis: Hash
            redisTemplate.opsForHash().put(key, "lastMessage", lastMessage != null ? lastMessage : "");
            redisTemplate.opsForHash().put(key, "title", title != null ? title : "");
            redisTemplate.opsForHash().put(key, "roomType", roomType != null ? roomType.name() : "DIRECT");
            redisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis setRoomMeta 실패 - roomId: {}", roomId, e);
        }
    }

    /** [Redis 캐시] 메시지 저장 시 room:meta의 updatedAt / lastMessage만 갱신 */
    public void updateRoomMetaOnNewMessage(String roomId, LocalDateTime updatedAt, String lastMessage) {
        try {
            String key = roomMetaKey(roomId);
            redisTemplate.opsForHash().put(key, "updatedAt", updatedAt.toString());  // Redis: Hash
            redisTemplate.opsForHash().put(key, "lastMessage", lastMessage != null ? lastMessage : "");
            redisTemplate.expire(key, TTL_DAYS);  // Redis: TTL
        } catch (Exception e) {
            logger.warn("Redis updateRoomMetaOnNewMessage 실패 - roomId: {}", roomId, e);
        }
    }

    /** [Redis 캐시] 내 채팅방 roomId 목록 ZREVRANGE (최신 순) */
    public List<String> getUserRoomIds(String email) {
        try {
            Set<String> set = stringRedisTemplate.opsForZSet().reverseRange(userRoomsKey(email), 0, -1);  // Redis: ZREVRANGE
            return set != null ? new ArrayList<>(set) : List.of();
        } catch (Exception e) {
            logger.debug("Redis getUserRoomIds 실패 - email: {}", email, e);
            return List.of();
        }
    }

    /** [Redis 캐시] 방 메타 Hash + last 메시지 Hash 조회. 없으면 empty map */
    public Map<String, String> getRoomMetaAndLast(String roomId) {
        try {
            Map<Object, Object> last = redisTemplate.opsForHash().entries(lastMessageKey(roomId));   // Redis: HGETALL (last)
            Map<Object, Object> meta = redisTemplate.opsForHash().entries(roomMetaKey(roomId));     // Redis: HGETALL (meta)
            Map<String, String> out = new HashMap<>();
            if (meta != null && !meta.isEmpty()) {
                meta.forEach((k, v) -> out.put(String.valueOf(k), v != null ? v.toString() : ""));
            }
            if (last != null && !last.isEmpty()) {
                Object content = last.get("content");
                if (content != null) out.put("lastMessage", content.toString());
                Object createdAt = last.get("createdAt");
                if (createdAt != null) out.put("lastMessageAt", createdAt.toString());
            }
            return out;
        } catch (Exception e) {
            logger.debug("Redis getRoomMetaAndLast 실패 - roomId: {}", roomId, e);
            return Map.of();
        }
    }

    /** [Redis 캐시] DB 조회 결과로 Redis 캐시 워밍 — addUserRoom, setRoomMeta, unread, setTotalUnread */
    public void warmRoomListCache(String email, List<ChatRoomDTO> rooms) {
        if (rooms == null || rooms.isEmpty()) return;
        try {
            int totalUnread = 0;
            for (ChatRoomDTO r : rooms) {
                long score = r.getUpdatedAt() != null
                        ? r.getUpdatedAt().atZone(java.time.ZoneId.of("Asia/Seoul")).toInstant().toEpochMilli()
                        : System.currentTimeMillis();
                addUserRoom(email, r.getRoomId(), score);
                setRoomMeta(r.getRoomId(), r.getUpdatedAt(), r.getLastMessage(), r.getTitle(), r.getRoomType());
                if (r.getNotReadMessageCount() != null && r.getNotReadMessageCount() > 0) {
                    String uk = unreadKey(r.getRoomId(), email);
                    stringRedisTemplate.opsForValue().set(uk, String.valueOf(r.getNotReadMessageCount()));  // Redis: SET (unread)
                    stringRedisTemplate.expire(uk, TTL_DAYS);  // Redis: TTL
                    totalUnread += r.getNotReadMessageCount().intValue();
                }
            }
            setTotalUnread(email, totalUnread);
        } catch (Exception e) {
            logger.warn("Redis warmRoomListCache 실패 - email: {}", email, e);
        }
    }
}
