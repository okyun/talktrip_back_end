package com.talktrip.talktrip.domain.chat.service;

import com.talktrip.talktrip.domain.chat.dto.request.ChatMessageRequestDto;
import com.talktrip.talktrip.domain.chat.dto.request.ChatRoomRequestDto;
import com.talktrip.talktrip.domain.chat.dto.response.*;
import com.talktrip.talktrip.domain.chat.dto.response.ChatMessagePush;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import com.talktrip.talktrip.domain.chat.entity.ChatRoom;
import com.talktrip.talktrip.domain.chat.entity.ChatRoomAccount;
import com.talktrip.talktrip.domain.chat.message.dto.ChatRoomUpdateMessage;
import com.talktrip.talktrip.domain.chat.repository.ChatMessageRepository;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomMemberRepository;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomRepository;
import com.talktrip.talktrip.global.dto.SliceResponse;
import com.talktrip.talktrip.global.redis.RedisMessageBroker;
import com.talktrip.talktrip.global.util.CursorUtil;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.security.Principal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
/**
 * 채팅 서비스 - 선택적 Spring Cache 적용
 * 
 * 캐시 전략:
 * - roomDetails: 채팅방 상세 정보 캐시 (멤버 수, 참여자 목록 등)
 * - existingRooms: 기존 채팅방 조회 결과 캐시 (사용자 간 채팅방 존재 여부)
 * 
 * 실시간 데이터 (캐시 제외):
 * - chatRooms: 사용자별 채팅방 목록 (마지막 메시지, 상태 정보 포함)
 * - unreadCounts: 읽지 않은 메시지 개수 (실시간 알림을 위해)
 * - chatHistory: 채팅 기록 (실시간 메시지 조회)
 * 
 * 캐시 무효화 시점:
 * - 채팅방 멤버 변경 시 roomDetails 캐시 무효화
 * - 새 메시지 전송 시 멤버 활성화로 인한 roomDetails 무효화
 * - 새 채팅방 생성 시 existingRooms 캐시 무효화
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChatService {

    private final ChatRoomRepository chatRoomRepository;
    private final ChatMessageRepository chatMessageRepository;
    private final ChatRoomMemberRepository chatRoomMemberRepository;
    private final RedisMessageBroker redisMessageBroker;
    private final StringRedisTemplate stringRedisTemplate;
    private final WebSocketSessionManager webSocketSessionManager;
    private final ChatMessageSequenceService chatMessageSequenceService;
    private final ChatMessageCacheService chatMessageCacheService;

    /**
     * 채팅 메시지 저장 및 전송 - 관련 캐시 무효화
     *
     * 캐시 무효화:
     * - roomDetails: 해당 채팅방의 상세 정보 (멤버 활성화로 인한 변경 가능성)
     *
     * 주의: 채팅방 목록, unread 카운트는 실시간 데이터로 별도 캐시하지 않음
     */
    @CacheEvict(value = "roomDetails", key = "#dto.roomId")
    @Transactional
    public void saveAndSend(ChatMessageRequestDto dto, Principal principal) {
        try {
            if (!isRedisAvailable()) {
                log.error("Redis 연결이 불가능합니다.");
                throw new RuntimeException("Redis 서버에 연결할 수 없습니다.");
            }
            final String sender = principal.getName();

            if (dto.getMessage() != null && dto.getMessage().contains("테스트에러")) {
                throw new RuntimeException("테스트용 에러: 메시지에 '테스트에러'가 포함되어 있습니다.");
            }

            // 1) 시퀀스 + DB 저장
            Long sequenceNumber = chatMessageSequenceService.getNextSequence(dto.getRoomId());
            ChatMessage entity = chatMessageRepository.save(dto.toEntity(sender, sequenceNumber));

            // 2) 채팅방 updatedAt 갱신
            chatRoomRepository.updateUpdatedAt(dto.getRoomId(), entity.getCreatedAt());

            // 3) push DTO
            ChatMessagePush push = ChatMessagePush.builder()
                    .messageId(entity.getMessageId())
                    .roomId(entity.getRoomId())
                    .sender(sender)
                    .senderName(sender.split("@")[0])
                    .message(entity.getMessage())
                    .createdAt(String.valueOf(entity.getCreatedAt()))
                    .build();

            // 4) 멤버 이메일 조회
            List<String> memberEmails = chatRoomMemberRepository
                    .findAllAccountEmailsByRoomId(dto.getRoomId())
                    .stream().map(ChatRoomAccount::getAccountEmail).toList();

            // 5) 사이드바 DTO (여기는 DB 기반 unread 사용)
            List<ChatRoomUpdateMessage> sidebars = new ArrayList<>(memberEmails.size());
            for (String email : memberEmails) {
                int unreadForThisUser = email.equals(sender)
                        ? 0
                        : chatMessageRepository.countUnreadMessagesByRoomIdAndMemberId(dto.getRoomId(), email);

                sidebars.add(ChatRoomUpdateMessage.builder()
                        .accountEmail(email)
                        .roomId(dto.getRoomId())
                        .messageId(entity.getMessageId())
                        .message(entity.getMessage())
                        .senderAccountEmail(sender)
                        .createdAt(entity.getCreatedAt())
                        .notReadMessageCount(unreadForThisUser)
                        .receiverAccountEmail(email)
                        .updatedAt(SeoulTimeUtil.nowAsTimestamp())
                        .unreadCountForSender(0)
                        .unreadCountForReceiver(unreadForThisUser)
                        .build());
            }

            // 6) 로컬 세션에 즉시 전송
            sendToLocalSessions(dto.getRoomId(), push);
            sendSidebarUpdatesToLocalSessions(memberEmails, sidebars);

            // 7) 커밋 이후에만 캐시 + 브로드캐스트
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    try {
                        // lastMessage 캐싱
                        chatMessageCacheService.cacheLastMessage(entity, principal.getName());

                        // 수신자 unread INCR
                        for (String email : memberEmails) {
                            if (!email.equals(sender)) {
                                chatMessageCacheService.incrementUnread(dto.getRoomId(), email);
                            }
                        }

                        // Redis 브로드캐스트 (다른 인스턴스용)
                        publishToRedis(dto, push, memberEmails, sidebars);

                    } catch (Exception ex) {
                        log.error("afterCommit에서 Redis 관련 처리 실패 - roomId: {}, error: {}",
                                dto.getRoomId(), ex.getMessage(), ex);
                    }
                }
            });

        } catch (AccessDeniedException e) {
            log.error("채팅방 접근 권한 없음: {}", e.getMessage(), e);
            throw new RuntimeException("채팅방에 접근할 권한이 없습니다.");
        } catch (IllegalArgumentException e) {
            log.error("잘못된 메시지 데이터: {}", e.getMessage(), e);
            throw new RuntimeException("잘못된 메시지 형식입니다: " + e.getMessage());
        } catch (Exception e) {
            log.error("채팅 메시지 저장 및 발행 중 오류 발생: {}", e.getMessage(), e);
            throw new RuntimeException("채팅 처리 중 오류가 발생했습니다: " + e.getMessage());
        }

        chatRoomMemberRepository.resetIsDelByRoomId(dto.getRoomId());
    }

    private void sendToLocalSessions(String roomId, ChatMessagePush push) {
        try {
            log.info("로컬 세션에 즉시 메시지 전송 시작 - roomId: {}, messageId: {}", roomId, push.getMessageId());
            webSocketSessionManager.sendChatMessagePushToLocalRoom(roomId, push);
            log.info("로컬 세션 즉시 전송 완료 - roomId: {}", roomId);
        } catch (Exception e) {
            log.error("로컬 세션 즉시 전송 실패 - roomId: {}, error: {}", roomId, e.getMessage(), e);
        }
    }

    private void sendSidebarUpdatesToLocalSessions(List<String> memberEmails, List<ChatRoomUpdateMessage> sidebars) {
        try {
            log.info("로컬 세션에 사이드바 업데이트 전송 시작 - 멤버 수: {}", memberEmails.size());
            for (int i = 0; i < memberEmails.size(); i++) {
                String email = memberEmails.get(i);
                ChatRoomUpdateMessage sidebar = sidebars.get(i);
                webSocketSessionManager.sendSidebarUpdateToLocalUser(email, sidebar);
            }
            log.info("로컬 세션 사이드바 업데이트 전송 완료");
        } catch (Exception e) {
            log.error("로컬 세션 사이드바 업데이트 전송 실패: {}", e.getMessage(), e);
        }
    }

    private void publishToRedis(ChatMessageRequestDto dto, ChatMessagePush push,
                                List<String> memberEmails, List<ChatRoomUpdateMessage> sidebars) {
        try {
            log.info("Redis 브로드캐스트 시작 - roomId: {}, 자신 제외", dto.getRoomId());

            redisMessageBroker.publishToOtherInstances("chat:room:" + dto.getRoomId(), push);
            publishSidebarUpdates(memberEmails, sidebars);

            log.info("Redis 브로드캐스트 완료 - roomId: {}", dto.getRoomId());
        } catch (Exception e) {
            log.error("Redis 브로드캐스트 실패: {}", e.getMessage(), e);
        }
    }

    private void publishSidebarUpdates(List<String> memberEmails, List<ChatRoomUpdateMessage> sidebars) {
        for (int i = 0; i < memberEmails.size(); i++) {
            String email = memberEmails.get(i);
            ChatRoomUpdateMessage sidebar = sidebars.get(i);
            redisMessageBroker.publishToOtherInstances("chat:user:" + email, sidebar);
        }
    }

    private boolean isRedisAvailable() {
        try {
            stringRedisTemplate.opsForValue().get("health_check");
            return true;
        } catch (Exception e) {
            log.warn("Redis 연결 확인 실패: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 채팅방 입장 또는 생성 (Command)
     */
    @Caching(evict = {
            @CacheEvict(value = "roomDetails", allEntries = true),
            @CacheEvict(value = "existingRooms", key = "#principal.name + ':' + #chatRoomRequestDto.sellerAccountEmail")
    })
    @Transactional
    public String enterOrCreateRoom(Principal principal, ChatRoomRequestDto chatRoomRequestDto) {
        String accountEmail = principal.getName();
        String sellerAccountEmail = chatRoomRequestDto.getSellerAccountEmail();

        Optional<String> existingRoom = findExistingRoom(accountEmail, sellerAccountEmail);
        if (existingRoom.isPresent()) {
            String roomId = existingRoom.get();

            if (webSocketSessionManager.isUserOnlineLocally(accountEmail)) {
                webSocketSessionManager.joinRoom(accountEmail, roomId);
            }
            return roomId;
        }

        ChatRoomResponseDto newRoomDto = ChatRoomResponseDto.createNew();
        String newRoomId = newRoomDto.getRoomId();

        ChatRoom chatRoom = ChatRoom.builder()
                .roomId(newRoomId)
                .productId(chatRoomRequestDto.getProductId())
                .build();
        chatRoomRepository.save(chatRoom);

        ChatRoomAccount buyerMember = ChatRoomAccount.create(newRoomId, accountEmail);
        ChatRoomAccount sellerMember = ChatRoomAccount.create(newRoomId, sellerAccountEmail);
        chatRoomMemberRepository.save(buyerMember);
        chatRoomMemberRepository.save(sellerMember);

        if (webSocketSessionManager.isUserOnlineLocally(accountEmail)) {
            webSocketSessionManager.joinRoom(accountEmail, newRoomId);
        }
        if (webSocketSessionManager.isUserOnlineLocally(sellerAccountEmail)) {
            webSocketSessionManager.joinRoom(sellerAccountEmail, newRoomId);
        }

        return newRoomId;
    }

    @Cacheable(
            value = "existingRooms",
            key = "#buyerEmail + ':' + #sellerEmail",
            unless = "!#result.isPresent()"
    )
    public Optional<String> findExistingRoom(String buyerEmail, String sellerEmail) {
        return chatRoomMemberRepository.findRoomIdByBuyerIdAndSellerId(buyerEmail, sellerEmail);
    }

    @CacheEvict(value = "roomDetails", key = "#roomId")
    @Transactional
    public void markChatRoomAsDeleted(String accountEmail, String roomId) {
        chatRoomMemberRepository.updateIsDelByMemberIdAndRoomId(accountEmail, roomId, 1);
    }

    @Transactional
    public void markRoomAsRead(String accountEmail, String roomId) {
        if (accountEmail == null || accountEmail.trim().isEmpty()) {
            throw new IllegalArgumentException("accountEmail cannot be null or empty");
        }
        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId cannot be null or empty");
        }

        boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, accountEmail);
        if (!isMember) {
            throw new AccessDeniedException("Not a member of this room: " + roomId);
        }

        int updatedRows = chatRoomMemberRepository.updateLastReadTime(roomId, accountEmail, SeoulTimeUtil.now());

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                chatMessageCacheService.clearUnread(roomId, accountEmail);
            }
        });

        if (updatedRows == 0) {
            log.warn("읽음 처리 실패 - 사용자: {}, 채팅방: {}", accountEmail, roomId);
            throw new RuntimeException("읽음 처리에 실패했습니다. 해당 사용자가 채팅방의 멤버가 아닙니다.");
        } else {
            log.info("읽음 처리 완료 - 사용자: {}, 채팅방: {}, 업데이트된 행: {}",
                    accountEmail, roomId, updatedRows);
        }
    }
    /**
     * 채팅방 메시지 기록 조회 및 읽음 처리 - 권한 검사 포함
     *
     * 주의: 읽음 처리로 인한 실시간 데이터 변경은 DB에서 직접 반영됨
     * 캐시 무효화 불필요 (chatRooms, unreadCounts는 실시간 데이터로 캐시하지 않음)
     *
     * @param roomId 조회할 채팅방 ID
     * @param accountEmail 요청한 사용자의 이메일 (권한 검사용)
     * @param limit 페이지 크기
     * @param cursor 페이지네이션 커서
     * @return 채팅 메시지 목록
     * @throws AccessDeniedException 사용자가 해당 채팅방의 멤버가 아닌 경우
     */
    @org.springframework.transaction.annotation.Transactional
    public SliceResponse<ChatMemberRoomWithMessageDto> getRoomChattingHistoryAndMarkAsRead(
            String roomId,
            String accountEmail,
            Integer limit,
            String cursor
    ) {
        // 1) 사용자가 해당 채팅방의 멤버인지 권한 검사
        boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, accountEmail);
        if (!isMember) {
            throw new AccessDeniedException("Not a member of this room: " + roomId);
        }

        // 2) page size 정규화
        final int size = (limit == null || limit <= 0 || limit > 200) ? 50 : limit;

        // 3) 정렬: 서버는 항상 최신→과거 (DESC)로 가져온다 - 시퀀스 기반
        var sort = Sort.by(Sort.Direction.DESC, "sequenceNumber", "createdAt", "messageId");
        var pageable = PageRequest.of(0, size, sort);

        // 4) 메시지 조회 (첫 진입 vs 커서 이전) - 시퀀스 기반
        List<ChatMessage> entities;
        if (cursor == null || cursor.isBlank()) {
            // 첫 페이지
            entities = chatMessageRepository.findFirstPage(roomId, pageable);
        } else {
            // 커서 이전 페이지 - 시퀀스 기반
            var c = CursorUtil.decode(cursor); // sequenceNumber + createdAt + messageId
            entities = chatMessageRepository.findSliceBefore(
                    roomId, Long.parseLong(c.messageId()), pageable // messageId 자리에 sequenceNumber 저장
            );
        }

        // 5) 읽음 처리 (내 lastReadAt 갱신)
        chatRoomMemberRepository.updateLastReadTime(roomId, accountEmail, SeoulTimeUtil.now());

        // 6) DTO 매핑
        var items = entities.stream()
                .map(ChatMemberRoomWithMessageDto::from)
                .toList();

        // 7) nextCursor/hasNext 계산 - 시퀀스 기반
        String nextCursor = null;
        boolean hasNext = false;
        if (!entities.isEmpty()) {
            var last = entities.get(entities.size() - 1);
            nextCursor = CursorUtil.encode(last.getCreatedAt(), last.getSequenceNumber().toString());
            hasNext = (entities.size() == size);
        }

        return SliceResponse.of(items, hasNext ? nextCursor : null, hasNext);
    }

}

//| 영역                          | 책임                                           | 어디서 사용?                      |
//        | --------------------------- | -------------------------------------------- | ---------------------------- |
//        | **ChatService**             | 메시지 저장, 브로드캐스트, 캐시 갱신                        | Write(Command)               |
//        | **ChatMessageCacheService** | lastMessage, unreadCount 캐싱/TTL, Redis 해시 관리 | Write(Command) + Query(Read) |
//        | **ChatRoomQueryService**    | 채팅방 목록/상세 조회, Redis 값 조합, DTO 완성             | Read(Query)                  |
