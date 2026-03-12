package com.talktrip.talktrip.domain.chat.service;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDTO;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDetailDto;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDetailScalar;
import com.talktrip.talktrip.domain.chat.enums.RoomType;
import com.talktrip.talktrip.domain.chat.repository.ChatMessageRepository;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomMemberRepository;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomRepository;
import com.talktrip.talktrip.global.dto.SliceResponse;
import com.talktrip.talktrip.global.util.CursorUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ChatRoomQueryService {

    private static final Logger logger = LoggerFactory.getLogger(ChatRoomQueryService.class);

    private final ChatRoomRepository chatRoomRepository;
    private final ChatMessageRepository chatMessageRepository;
    private final ChatRoomMemberRepository chatRoomMemberRepository;
    private final ChatMessageCacheService chatMessageCacheService;

    /**
     * 사용자별 채팅방 목록 조회 + 커서 페이징
     * Redis 우선 조회, 없으면 DB fallback + 캐시 워밍. DB 결과의 notReadMessageCount는 Redis 값으로 보정.
     */
    public SliceResponse<ChatRoomDTO> getRooms(
            String accountEmail,
            Integer limit,
            String cursor
    ) {
        final int size = (limit == null || limit <= 0 || limit > 100) ? 25 : limit;

        List<ChatRoomDTO> allRooms = getRoomsFromRedis(accountEmail);
        if (allRooms.isEmpty()) {
            allRooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);
            allRooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));
            overrideNotReadFromRedis(accountEmail, allRooms);
            chatMessageCacheService.warmRoomListCache(accountEmail, allRooms);
        }

        List<ChatRoomDTO> rooms;
        if (cursor == null || cursor.isBlank()) {
            rooms = new ArrayList<>(allRooms.stream().limit(size).toList());
        } else {
            try {
                var c = CursorUtil.decode(cursor);
                String cursorRoomId = c.messageId();
                int startIndex = -1;
                for (int i = 0; i < allRooms.size(); i++) {
                    if (allRooms.get(i).getRoomId().equals(cursorRoomId)) {
                        startIndex = i + 1;
                        break;
                    }
                }
                if (startIndex >= 0 && startIndex < allRooms.size()) {
                    rooms = new ArrayList<>(allRooms.stream().skip(startIndex).limit(size).toList());
                } else {
                    rooms = new ArrayList<>();
                }
            } catch (Exception e) {
                rooms = new ArrayList<>();
            }
        }

        rooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));

        String nextCursor = null;
        boolean hasNext = false;
        if (!rooms.isEmpty()) {
            var last = rooms.get(rooms.size() - 1);
            nextCursor = CursorUtil.encode(last.getUpdatedAt(), last.getRoomId());
            hasNext = (rooms.size() == size);
        }

        return SliceResponse.of(rooms, hasNext ? nextCursor : null, hasNext);
    }

    /** Redis에서 채팅방 목록 구성 (user:rooms ZSET + room meta + unread) */
    private List<ChatRoomDTO> getRoomsFromRedis(String accountEmail) {
        List<String> roomIds = chatMessageCacheService.getUserRoomIds(accountEmail);
        if (roomIds.isEmpty()) return List.of();

        List<ChatRoomDTO> result = new ArrayList<>();
        for (String roomId : roomIds) {
            ChatRoomDTO dto = buildChatRoomDTOFromRedis(roomId, accountEmail);
            if (dto != null) result.add(dto);
        }
        return result;
    }

    private ChatRoomDTO buildChatRoomDTOFromRedis(String roomId, String accountEmail) {
        try {
            Map<String, String> meta = chatMessageCacheService.getRoomMetaAndLast(roomId);
            if (meta.isEmpty()) return null;

            String updatedAtStr = meta.get("updatedAt");
            if (updatedAtStr == null) updatedAtStr = meta.get("lastMessageAt");
            LocalDateTime updatedAt = parseLocalDateTime(updatedAtStr);
            LocalDateTime createdAt = updatedAt;

            String title = meta.getOrDefault("title", "");
            String lastMessage = meta.getOrDefault("lastMessage", "");
            RoomType roomType = parseRoomType(meta.get("roomType"));

            int unread = chatMessageCacheService.getUnread(roomId, accountEmail).orElse(0);

            return new ChatRoomDTO(
                    roomId, "", createdAt, updatedAt, title, lastMessage,
                    (long) unread, roomType
            );
        } catch (Exception e) {
            logger.debug("buildChatRoomDTOFromRedis 실패 - roomId: {}", roomId, e);
            return null;
        }
    }

    private static LocalDateTime parseLocalDateTime(String s) {
        if (s == null || s.isBlank()) return LocalDateTime.now();
        try {
            return LocalDateTime.parse(s.replace(" ", "T"));
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }

    private static RoomType parseRoomType(String s) {
        if (s == null || s.isBlank()) return RoomType.DIRECT;
        try {
            return RoomType.valueOf(s);
        } catch (Exception e) {
            return RoomType.DIRECT;
        }
    }

    /** DB 조회 결과 리스트의 notReadMessageCount를 Redis 값으로 덮어씀 */
    private void overrideNotReadFromRedis(String accountEmail, List<ChatRoomDTO> rooms) {
        for (ChatRoomDTO r : rooms) {
            chatMessageCacheService.getUnread(r.getRoomId(), accountEmail)
                    .ifPresent(count -> r.setNotReadMessageCount((long) count));
        }
    }

    /**
     * 채팅방 전체 목록 (필터 X). Redis 우선, 없으면 DB + 워밍.
     */
    public List<ChatRoomDTO> getAllRooms(String accountEmail) {
        List<ChatRoomDTO> rooms = getRoomsFromRedis(accountEmail);
        if (rooms.isEmpty()) {
            rooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);
            rooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));
            overrideNotReadFromRedis(accountEmail, rooms);
            chatMessageCacheService.warmRoomListCache(accountEmail, rooms);
        }
        return rooms;
    }

    /**
     * roomType 필터 버전. Redis 우선, 없으면 DB + 워밍 후 필터.
     */
    public List<ChatRoomDTO> getAllRooms(String accountEmail, String roomType) {
        List<ChatRoomDTO> allRooms = getAllRooms(accountEmail);

        if (roomType == null || roomType.isEmpty()) {
            return allRooms;
        }

        try {
            var filterType = RoomType.valueOf(roomType.toUpperCase());
            return allRooms.stream()
                    .filter(room -> room.getRoomType() == filterType)
                    .toList();
        } catch (IllegalArgumentException e) {
            logger.warn("잘못된 roomType: {}, 전체 목록 반환", roomType);
            return allRooms;
        }
    }

    /**
     * 읽지 않은 메시지가 있는 채팅방 수
     */
    public int getCountAllUnreadMessageRooms(String accountEmail) {
        return chatMessageRepository.countUnreadMessagesRooms(accountEmail);
    }

    /**
     * 전체 읽지 않은 메시지 개수. Redis 우선, 없으면 DB 조회 후 Redis 워밍.
     */
    public int getCountAllUnreadMessages(String accountEmail) {
        Optional<Integer> cached = chatMessageCacheService.getTotalUnread(accountEmail);
        if (cached.isPresent()) {
            return cached.get();
        }
        int fromDb = chatMessageRepository.countUnreadMessages(accountEmail);
        chatMessageCacheService.setTotalUnread(accountEmail, fromDb);
        return fromDb;
    }

    /**
     * 특정 방의 읽지 않은 메시지 개수
     */
    public int getCountUnreadMessagesByRoomId(String roomId, String accountEmail) {
        return chatMessageRepository.countUnreadMessagesByRoomId(roomId, accountEmail);
    }

    /**
     * 채팅방 상세 정보 조회 (@Cacheable 유지)
     */
    @Cacheable(value = "roomDetails", key = "#roomId", unless = "#result == null")
    public ChatRoomDetailDto getRoomDetail(String roomId, String email) {
        ChatRoomDetailScalar s = chatRoomRepository.findRoomScalar(roomId)
                .orElseThrow(() -> new IllegalArgumentException("room not found: " + roomId));

        boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, email);
        if (!isMember) {
            throw new AccessDeniedException("Not a member of this room: " + roomId);
        }

        var roomUpdatedAt = chatRoomRepository.findChatRoomUpdateAtByRoomId(roomId);
        var memberCount = chatRoomMemberRepository.countMembers(roomId);
        var participants = chatRoomMemberRepository.findParticipantEmails(roomId);

        return new ChatRoomDetailDto(
                s.roomId(),
                s.title(),
                s.productId(),
                null,
                roomUpdatedAt,
                memberCount,
                participants
        );
    }
}