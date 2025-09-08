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
//    private final SimpMessagingTemplate messagingTemplate;
    private final ChatRoomMemberRepository chatRoomMemberRepository;
//    private final ChannelTopic topic;//Spring Data Redis에서 Pub/Sub 구조에서 사용하는 "채널 이름"
//    private final ChannelTopic roomUpdateTopic;
    private final RedisMessageBroker redisMessageBroker;
    private final StringRedisTemplate stringRedisTemplate;
    private final WebSocketSessionManager webSocketSessionManager;
    private final ChatMessageSequenceService chatMessageSequenceService;


    /**
     * 채팅 메시지 저장 및 전송 - 관련 캐시 무효화
     * 
     * 캐시 무효화:
     * - roomDetails: 해당 채팅방의 상세 정보 (멤버 활성화로 인한 변경 가능성)
     * 
     * 주의: chatRooms, unreadCounts는 실시간 데이터로 캐시하지 않으므로 무효화 불필요
     */
    @CacheEvict(value = "roomDetails", key = "#dto.roomId")
    @Transactional
    public void saveAndSend(ChatMessageRequestDto dto, Principal principal) {
        try {
            // Redis 연결 상태 미리 확인 (메시지 저장 전에 체크)
            if (!isRedisAvailable()) {
                log.error("Redis 연결이 불가능합니다.");
                throw new RuntimeException("Redis 서버에 연결할 수 없습니다.");
            }
            final String sender = principal.getName();
            
            // 테스트용: 특정 메시지로 에러 발생 (실제 운영에서는 제거)
            if (dto.getMessage() != null && dto.getMessage().contains("테스트에러")) {
                throw new RuntimeException("테스트용 에러: 메시지에 '테스트에러'가 포함되어 있습니다.");
            }

            // (선택) 권한 체크
            // if (!chatRoomMemberRepository.existsByRoomIdAndAccountEmail(dto.getRoomId(), sender)) {
            //     throw new AccessDeniedException("Not a member of this room");
            // }

            // 1) Redis 기반 채팅방 시퀀스 생성 (원자적 연산)
            Long sequenceNumber = chatMessageSequenceService.getNextSequence(dto.getRoomId());

            // 2) DB 저장 (시퀀스 번호 포함)
            ChatMessage entity = chatMessageRepository.save(dto.toEntity(sender, sequenceNumber));
            
            // 2) ChatRoom의 updatedAt 업데이트 (최신 메시지 시간으로)
            chatRoomRepository.updateUpdatedAt(dto.getRoomId(), entity.getCreatedAt());

            // 2) 방 브로드캐스트 payload
            ChatMessagePush push = ChatMessagePush.builder()
                    .messageId(entity.getMessageId())
                    .roomId(entity.getRoomId())
                    .sender(sender)
                    .senderName(sender.split("@")[0])
                    .message(entity.getMessage())
                    .createdAt(String.valueOf(entity.getCreatedAt()))
                    .build();

            // 3) 개인 사이드바 payload들 미리 계산 (트랜잭션 안에서 조회 OK)
            List<String> memberEmails = chatRoomMemberRepository
                    .findAllAccountEmailsByRoomId(dto.getRoomId())
                    .stream().map(ChatRoomAccount::getAccountEmail).toList();

            List<ChatRoomUpdateMessage> sidebars = new ArrayList<>(memberEmails.size());
            for (String email : memberEmails) {
                int unreadForThisUser = email.equals(sender)
                        ? 0
                        : chatMessageRepository.countUnreadMessagesByRoomIdAndMemberId(dto.getRoomId(), email);

                sidebars.add(ChatRoomUpdateMessage.builder()
                        .accountEmail(email)                    // 수신자 이메일 (누가 받을지)
                        .roomId(dto.getRoomId())
                        .messageId(entity.getMessageId())
                        .message(entity.getMessage())
                        .senderAccountEmail(sender)             // 발신자 이메일
                        .createdAt(entity.getCreatedAt())
                        .notReadMessageCount(unreadForThisUser) // 읽지 않은 메시지 개수
                        .receiverAccountEmail(email)            // 수신자 이메일 (중복이지만 필수 필드)
                        .updatedAt(SeoulTimeUtil.nowAsTimestamp())
                        .unreadCountForSender(0)                // 발신자는 항상 0
                        .unreadCountForReceiver(unreadForThisUser)
                        .build());
            }

            // 4) 로컬 세션에 즉시 전송 (실시간 응답성 보장)
            sendToLocalSessions(dto.getRoomId(), push);
            
            // 5) 로컬 세션에 사이드바 업데이트 즉시 전송
            sendSidebarUpdatesToLocalSessions(memberEmails, sidebars);

            // 5) ❗ DB 커밋이 "성공한 뒤에만" Redis로 다른 서버 인스턴스에 브로드캐스트
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() {
                    publishToRedis(dto, push, memberEmails, sidebars);
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

        // (선택) 기타 후처리
        chatRoomMemberRepository.resetIsDelByRoomId(dto.getRoomId());
    }

    /**
     * 로컬 WebSocket 세션에 즉시 메시지 전송 (실시간 응답성 보장)
     * 
     * @param roomId 채팅방 ID
     * @param push ChatMessagePush DTO
     */
    private void sendToLocalSessions(String roomId, ChatMessagePush push) {
        try {
            log.info("로컬 세션에 즉시 메시지 전송 시작 - roomId: {}, messageId: {}", roomId, push.getMessageId());
            
            // WebSocketSessionManager를 통해 로컬 세션들에게 즉시 전송
            webSocketSessionManager.sendChatMessagePushToLocalRoom(roomId, push);
            
            log.info("로컬 세션 즉시 전송 완료 - roomId: {}", roomId);
            
        } catch (Exception e) {
            log.error("로컬 세션 즉시 전송 실패 - roomId: {}, error: {}", roomId, e.getMessage(), e);
            // 로컬 전송 실패는 Redis 브로드캐스트로 보완되므로 예외를 던지지 않음
        }
    }

    /**
     * 로컬 세션에 사이드바 업데이트 즉시 전송
     * 
     * @param memberEmails 채팅방 멤버 이메일 목록
     * @param sidebars 사이드바 업데이트 메시지 목록
     */
    private void sendSidebarUpdatesToLocalSessions(List<String> memberEmails, List<ChatRoomUpdateMessage> sidebars) {
        try {
            log.info("로컬 세션에 사이드바 업데이트 전송 시작 - 멤버 수: {}", memberEmails.size());
            
            for (int i = 0; i < memberEmails.size(); i++) {
                String email = memberEmails.get(i);
                ChatRoomUpdateMessage sidebar = sidebars.get(i);
                
                // WebSocketSessionManager를 통해 로컬 세션에 즉시 전송
                webSocketSessionManager.sendSidebarUpdateToLocalUser(email, sidebar);
            }
            
            log.info("로컬 세션 사이드바 업데이트 전송 완료");
            
        } catch (Exception e) {
            log.error("로컬 세션 사이드바 업데이트 전송 실패: {}", e.getMessage(), e);
            // 로컬 전송 실패는 Redis 브로드캐스트로 보완되므로 예외를 던지지 않음
        }
    }

    /**
     * Redis로 채팅 메시지와 사이드바 업데이트를 발행합니다.
     * DB 커밋 후 실행되므로 예외가 발생해도 트랜잭션에 영향을 주지 않습니다.
     * 다른 서버 인스턴스에만 브로드캐스트 (자신은 제외)
     */
    private void publishToRedis(ChatMessageRequestDto dto, ChatMessagePush push, 
                               List<String> memberEmails, List<ChatRoomUpdateMessage> sidebars) {
        try {
            log.info("Redis 브로드캐스트 시작 - roomId: {}, 자신 제외", dto.getRoomId());
            
            // 방 전체 브로드캐스트 → 다른 서버 인스턴스들에만 전송 (자신은 제외)
            redisMessageBroker.publishToOtherInstances("chat:room:" + dto.getRoomId(), push);

            // 개인별 사이드바 업데이트 → 다른 서버 인스턴스의 사용자들에게만 전송
            publishSidebarUpdates(memberEmails, sidebars);
            
            log.info("Redis 브로드캐스트 완료 - roomId: {}", dto.getRoomId());
            
        } catch (Exception e) {
            log.error("Redis 브로드캐스트 실패: {}", e.getMessage(), e);
            // afterCommit에서는 예외를 던져도 트랜잭션이 이미 커밋되어 WebSocket 컨트롤러에서 잡히지 않음
            // 대신 로그만 남기고, 클라이언트는 메시지가 성공적으로 저장되었다고 생각할 수 있음
            // 실제 운영에서는 Redis 모니터링이나 알림 시스템을 통해 처리해야 함
        }
    }

    /**
     * 각 사용자별로 사이드바 업데이트 메시지를 다른 서버 인스턴스에 발행합니다.
     */
    private void publishSidebarUpdates(List<String> memberEmails, List<ChatRoomUpdateMessage> sidebars) {
        for (int i = 0; i < memberEmails.size(); i++) {
            String email = memberEmails.get(i);
            ChatRoomUpdateMessage sidebar = sidebars.get(i);
            // 다른 서버 인스턴스의 사용자들에게만 전송 (자신은 제외)
            redisMessageBroker.publishToOtherInstances("chat:user:" + email, sidebar);
        }
    }

    /**
     * Redis 연결 상태 확인
     */
    private boolean isRedisAvailable() {
        try {
            // Redis 연결 상태를 간단한 명령으로 확인
            // 연결이 안되면 예외 발생
            stringRedisTemplate.opsForValue().get("health_check");
            return true;
        } catch (Exception e) {
            log.warn("Redis 연결 확인 실패: {}", e.getMessage());
            return false;
        }
    }
    public String createRoom(String userA, String userB) {
        // 기존 방 있으면 재사용, 없으면 새로 생성
        // room_id = UUID.ranㄴdomUUID().toString()
        return userA;
    }
    /**
     * 사용자별 채팅방 목록 조회 - 실시간 데이터 필요로 캐시 제거 (페이지네이션 적용)
     * 
     * 캐시 제거 이유:
     * - 마지막 메시지 정보가 실시간으로 변경됨
     * - 채팅방 상태 정보가 실시간으로 업데이트됨
     * - 사용자에게 항상 최신 정보를 제공해야 함
     */
    public SliceResponse<ChatRoomDTO> getRooms(
            String accountEmail,
            Integer limit,
            String cursor
    ) {
        // 1) page size 정규화
        final int size = (limit == null || limit <= 0 || limit > 100) ? 25 : limit;

        // 2) 페이지 크기만 사용 (메모리에서 페이지네이션 처리)

        // 3) 채팅방 목록 조회 (전체 조회 후 메모리에서 페이지네이션)
        List<ChatRoomDTO> allRooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);
        
        // updatedAt 내림차순으로 정렬 (최신 메시지 순)
        allRooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));
        
        List<ChatRoomDTO> rooms;
        if (cursor == null || cursor.isBlank()) {
            // 첫 페이지 - 처음부터 size개 가져오기
            rooms = new ArrayList<>(allRooms.stream().limit(size).toList());
        } else {
            // 커서 이후 페이지 - 커서 위치 찾아서 그 다음부터 size개 가져오기
            try {
                var c = CursorUtil.decode(cursor); // updatedAt + roomId
                String cursorRoomId = c.messageId(); // roomId가 messageId 자리에 저장됨
                
                int startIndex = -1;
                for (int i = 0; i < allRooms.size(); i++) {
                    if (allRooms.get(i).getRoomId().equals(cursorRoomId)) {
                        startIndex = i + 1; // 다음 인덱스부터 시작
                        break;
                    }
                }
                
                if (startIndex >= 0 && startIndex < allRooms.size()) {
                    rooms = new ArrayList<>(allRooms.stream().skip(startIndex).limit(size).toList());
                } else {
                    rooms = new ArrayList<>(); // 커서 위치를 찾을 수 없거나 끝에 도달
                }
            } catch (Exception e) {
                // 커서 파싱 실패 시 빈 목록 반환
                rooms = new ArrayList<>();
            }
        }

        // 4) DTO 정렬 (이미 정렬되어 있지만 안전성을 위해)
        rooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));

        // 5) nextCursor/hasNext 계산
        String nextCursor = null;
        boolean hasNext = false;
        if (!rooms.isEmpty()) {
            var last = rooms.get(rooms.size() - 1);
            nextCursor = CursorUtil.encode(last.getUpdatedAt(), last.getRoomId());
            hasNext = (rooms.size() == size); // 꽉 찼으면 더 있음으로 간주
        }

        return SliceResponse.of(rooms, hasNext ? nextCursor : null, hasNext);
    }

    /**
     * 기존 호환성을 위한 메소드 (모든 채팅방 반환)
     */
    public List<ChatRoomDTO> getAllRooms(String accountEmail) {
        List<ChatRoomDTO> rooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);
        
        // updatedAt 내림차순으로 정렬 (최신 메시지 순)
        rooms.sort((a, b) -> b.getUpdatedAt().compareTo(a.getUpdatedAt()));
        
        return rooms;
    }
    /**
     * 읽지 않은 메시지가 있는 채팅방 개수 조회 - 실시간 데이터 필요로 캐시 제거
     * 
     * 캐시 제거 이유:
     * - 읽지 않은 메시지 개수는 실시간으로 변경되는 중요한 정보
     * - 사용자의 읽음 처리에 따라 즉시 반영되어야 함
     * - 정확한 알림을 위해 항상 최신 데이터가 필요함
     */
    public int getCountALLUnreadMessagesRooms(String accountEmail) {
        return chatMessageRepository.countUnreadMessagesRooms( accountEmail);
    }
    /**
     * 전체 읽지 않은 메시지 개수 조회 - 실시간 데이터 필요로 캐시 제거
     * 
     * 캐시 제거 이유:
     * - 읽지 않은 메시지 개수는 실시간으로 변경되는 중요한 정보
     * - FloatingChatIcon 등에서 실시간 알림을 위해 사용됨
     * - 정확한 배지 표시를 위해 항상 최신 데이터가 필요함
     */
    public int getCountAllUnreadMessages(String accountEmail) {
        return chatMessageRepository.countUnreadMessages(accountEmail);
    }
    /**
     * 특정 채팅방의 읽지 않은 메시지 개수 조회 - 실시간 데이터 필요로 캐시 제거
     * 
     * 캐시 제거 이유:
     * - 채팅방별 읽지 않은 메시지 개수는 실시간으로 변경됨
     * - 채팅방 UI에서 실시간 배지 표시를 위해 사용됨
     * - 사용자의 읽음 처리 시 즉시 반영되어야 함
     */
    public  int getCountUnreadMessagesByRoomId(String roomId,String accountEmail) {
        return chatMessageRepository.countUnreadMessagesByRoomId(roomId,accountEmail);
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
    @Transactional
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
        // 서울 시간대를 사용하여 읽음 처리
        chatRoomMemberRepository.updateLastReadTime(roomId, accountEmail, SeoulTimeUtil.now());

        // 6) DTO 매핑
        var items = entities.stream()
                .map(ChatMemberRoomWithMessageDto::from) // ChatMessage -> ChatMemberRoomWithMessageDto 매핑
                .toList();


        // 7) nextCursor/hasNext 계산 - 시퀀스 기반
        String nextCursor = null;
        boolean hasNext = false;
        if (!entities.isEmpty()) {
            var last = entities.get(entities.size() - 1);
            nextCursor = CursorUtil.encode(last.getCreatedAt(), last.getSequenceNumber().toString());
            hasNext = (entities.size() == size); // 꽉 찼으면 더 있음으로 간주
        }

        return SliceResponse.of(items, hasNext ? nextCursor : null, hasNext);
    }
    /**
     * 채팅방 입장 또는 생성 - 관련 캐시 무효화 및 WebSocket 세션 갱신
     * 
     * 캐시 무효화:
     * - roomDetails: 새로 생성된 채팅방의 상세 정보 (멤버 수, 참여자 목록 변경)
     * - existingRooms: 새 채팅방 생성 시 기존 채팅방 조회 캐시 무효화
     * 
     * WebSocket 세션 갱신:
     * - 온라인 상태인 사용자들을 해당 채팅방에 자동 참가시킴
     * - 실시간 메시지 수신을 위한 세션 관리
     * 
     * 주의: chatRooms는 실시간 데이터로 캐시하지 않으므로 무효화 불필요
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
            
            // 기존 채팅방 입장 시에도 세션 갱신
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

        // Redis 시퀀스는 자동으로 0부터 시작하므로 별도 초기화 불필요

        // 생성자(구매자) 세션 갱신 - 현재 온라인 상태인 경우 새 채팅방에 참가시킴
        if (webSocketSessionManager.isUserOnlineLocally(accountEmail)) {
            webSocketSessionManager.joinRoom(accountEmail, newRoomId);
        }
        
        // 판매자 세션 갱신 - 현재 온라인 상태인 경우 새 채팅방에 참가시킴
        if (webSocketSessionManager.isUserOnlineLocally(sellerAccountEmail)) {
            webSocketSessionManager.joinRoom(sellerAccountEmail, newRoomId);
        }

        return newRoomId;
    }

    /**
     * 기존 채팅방 조회 - 캐시 적용
     * 
     * 캐시 키: buyerEmail + ':' + sellerEmail
     * 캐시 조건: 결과가 존재하는 경우만 캐시
     */
    @Cacheable(value = "existingRooms", key = "#buyerEmail + ':' + #sellerEmail", unless = "!#result.isPresent()")
    public Optional<String> findExistingRoom(String buyerEmail, String sellerEmail) {
        return chatRoomMemberRepository.findRoomIdByBuyerIdAndSellerId(buyerEmail, sellerEmail);
    }

    /**
     * 채팅방 삭제 처리 - 관련 캐시 무효화
     * 
     * 캐시 무효화:
     * - roomDetails: 해당 채팅방의 상세 정보 (멤버 수, 참여자 목록 변경 - 실질적으로 멤버가 나가는 것)
     * 
     * 주의: chatRooms, unreadCounts는 실시간 데이터로 캐시하지 않으므로 무효화 불필요
     */
    @CacheEvict(value = "roomDetails", key = "#roomId")
    @Transactional
    public void markChatRoomAsDeleted(String accountEmail, String roomId) {
        chatRoomMemberRepository.updateIsDelByMemberIdAndRoomId(accountEmail, roomId, 1);
    }
    /**
     * 채팅방 상세 정보 조회 - 캐시 적용 및 권한 검사
     * 
     * 캐시 키: roomId (채팅방별로 캐시)
     * 캐시 조건: 결과가 null이 아닌 경우만 캐시
     * 
     * @param roomId 조회할 채팅방 ID
     * @param email 요청한 사용자의 이메일 (권한 검사용)
     * @return 채팅방 상세 정보
     * @throws IllegalArgumentException 채팅방이 존재하지 않는 경우
     * @throws AccessDeniedException 사용자가 해당 채팅방의 멤버가 아닌 경우
     */
    @Cacheable(value = "roomDetails", key = "#roomId", unless = "#result == null")
    public ChatRoomDetailDto getRoomDetail(String roomId, String email) {
        // 1) 채팅방 존재 여부 확인
        var s = chatRoomRepository.findRoomScalar(roomId)
                .orElseThrow(() -> new IllegalArgumentException("room not found: " + roomId));

        // 2) 사용자가 해당 채팅방의 멤버인지 권한 검사
        boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, email);
        if (!isMember) {
            throw new AccessDeniedException("Not a member of this room: " + roomId);
        }

        // 3) 채팅방 메타 정보 조회
        var roomUpdatedAt = chatRoomRepository.findChatRoomUpdateAtByRoomId(roomId);
        var memberCount  = chatRoomMemberRepository.countMembers(roomId);
        var participants = chatRoomMemberRepository.findParticipantEmails(roomId);

        return new ChatRoomDetailDto(
                s.roomId(),      // 또는 s.getRoomId()
                s.title(),
                s.productId(),
                null,            // ownerEmail 필요시 r.roomAccountId도 JPQL에 추가하세요
                roomUpdatedAt,
                memberCount,
                participants
        );
    }

    /**
     * 채팅방 읽음 처리 (last_member_read_time 업데이트)
     * 
     * WHERE 절을 사용하여 room_id와 account_email로 필터링하여
     * 해당 사용자의 last_member_read_time을 현재 시간으로 업데이트
     * 
     * 그룹채팅과 1:1 채팅 모두에서 올바르게 작동하도록 구현
     * - 그룹채팅: 각 사용자별로 개별적인 읽음 처리
     * - 1:1 채팅: 상대방의 메시지만 카운트에서 제외
     * 
     * @param accountEmail 사용자 이메일 (WHERE 절 필터링 조건)
     * @param roomId 채팅방 ID (WHERE 절 필터링 조건)
     * @throws AccessDeniedException 사용자가 해당 채팅방의 멤버가 아닌 경우
     * @throws IllegalArgumentException 잘못된 파라미터가 전달된 경우
     */
    @Transactional
    public void markRoomAsRead(String accountEmail, String roomId) {
        // 1) 파라미터 유효성 검사
        if (accountEmail == null || accountEmail.trim().isEmpty()) {
            throw new IllegalArgumentException("accountEmail cannot be null or empty");
        }
        if (roomId == null || roomId.trim().isEmpty()) {
            throw new IllegalArgumentException("roomId cannot be null or empty");
        }

        // 2) 사용자가 해당 채팅방의 멤버인지 권한 검사
        boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, accountEmail);
        if (!isMember) {
            throw new AccessDeniedException("Not a member of this room: " + roomId);
        }

        // 3) 읽음 처리 (last_member_read_time 갱신)
        // WHERE room_id = :roomId AND account_email = :accountEmail 조건으로 필터링
        // 서울 시간대를 사용하여 읽음 처리
        int updatedRows = chatRoomMemberRepository.updateLastReadTime(roomId, accountEmail, SeoulTimeUtil.now());
        
        if (updatedRows == 0) {
            log.warn("읽음 처리 실패 - 사용자: {}, 채팅방: {} (WHERE 조건에 맞는 레코드가 없음)", accountEmail, roomId);
            throw new RuntimeException("읽음 처리에 실패했습니다. 해당 사용자가 채팅방의 멤버가 아닙니다.");
        } else {
            log.info("읽음 처리 완료 - 사용자: {}, 채팅방: {}, 업데이트된 행: {}", accountEmail, roomId, updatedRows);
        }
    }

    /**
     * 모든 채팅방 목록 조회 (roomType 필터 지원)
     * 
     * @param accountEmail 사용자 이메일
     * @param roomType 필터링할 채팅방 타입 (DIRECT, GROUP, null=전체)
     * @return 채팅방 목록
     */
    public List<ChatRoomDTO> getAllRooms(String accountEmail, String roomType) {
        List<ChatRoomDTO> allRooms = chatRoomRepository.findRoomsWithLastMessageByMemberId(accountEmail);
        
        if (roomType == null || roomType.isEmpty()) {
            return allRooms;
        }
        
        try {
            com.talktrip.talktrip.domain.chat.enums.RoomType filterType = 
                com.talktrip.talktrip.domain.chat.enums.RoomType.valueOf(roomType.toUpperCase());
            return allRooms.stream()
                    .filter(room -> room.getRoomType() == filterType)
                    .collect(java.util.stream.Collectors.toList());
        } catch (IllegalArgumentException e) {
            log.warn("잘못된 roomType: {}, 전체 목록 반환", roomType);
            return allRooms;
        }
    }
}