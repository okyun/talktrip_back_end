package com.talktrip.talktrip.domain.chat.service;
import com.talktrip.talktrip.domain.chat.dto.response.ChatMemberRoomWithMessageDto;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDTO;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDetailDto;
import com.talktrip.talktrip.domain.chat.dto.response.ChatRoomDetailScalar;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import com.talktrip.talktrip.domain.chat.repository.ChatMessageRepository;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomMemberRepository;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomRepository;
import com.talktrip.talktrip.global.dto.SliceResponse;
import com.talktrip.talktrip.global.util.CursorUtil;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ChatRoomQueryService {

    private final ChatRoomRepository chatRoomRepository;
    private final ChatMessageRepository chatMessageRepository;
    private final ChatRoomMemberRepository chatRoomMemberRepository;

    /**
     * 사용자별 채팅방 목록 조회 + 커서 페이징
     * (lastMessage, notReadMessageCount는 JPQL에서 계산된 DTO 사용)
     */
    public SliceResponse<ChatRoomDTO> getRooms(
            String accountEmail,
            Integer limit,
            String cursor
    ) {
        final int size = (limit == null || limit <= 0 || limit > 100) ? 25 : limit;

        List<ChatRoomDTO> allRooms =
                chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);

        // updatedAt DESC 정렬
        allRooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));

        List<ChatRoomDTO> rooms;
        if (cursor == null || cursor.isBlank()) {
            rooms = new ArrayList<>(allRooms.stream().limit(size).toList());
        } else {
            try {
                var c = CursorUtil.decode(cursor); // updatedAt + roomId
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

    /**
     * 채팅방 전체 목록 (필터 X)
     */
    public List<ChatRoomDTO> getAllRooms(String accountEmail) {
        List<ChatRoomDTO> rooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);
        rooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));
        return rooms;
    }

    /**
     * roomType 필터 버전
     */
    public List<ChatRoomDTO> getAllRooms(String accountEmail, String roomType) {
        List<ChatRoomDTO> allRooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);

        if (roomType == null || roomType.isEmpty()) {
            return allRooms;
        }

        try {
            var filterType =
                    com.talktrip.talktrip.domain.chat.enums.RoomType.valueOf(roomType.toUpperCase());
            return allRooms.stream()
                    .filter(room -> room.getRoomType() == filterType)
                    .toList();
        } catch (IllegalArgumentException e) {
            log.warn("잘못된 roomType: {}, 전체 목록 반환", roomType);
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
     * 전체 읽지 않은 메시지 개수
     */
    public int getCountAllUnreadMessages(String accountEmail) {
        return chatMessageRepository.countUnreadMessages(accountEmail);
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