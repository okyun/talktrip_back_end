package com.talktrip.talktrip.domain.chat.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import com.talktrip.talktrip.domain.chat.repository.ChatRoomMemberRepository;
import com.talktrip.talktrip.global.redis.RedisMessageBroker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketSessionManager { 

    private final RedisTemplate<String,Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final RedisMessageBroker redisMessageBroker;
    private final ChatRoomMemberRepository chatRoomMemberRepository;
    private final org.springframework.messaging.simp.SimpMessagingTemplate messagingTemplate;

    // 사용자 세션 관리: accountEmail -> WebSocketSession Set
    private final ConcurrentMap<String, java.util.Set<WebSocketSession>> userSessions = new ConcurrentHashMap<>();
    String serverRoomsKeyPrefix = "chat:room" ;
    String serverUsersKeyPrefix = "chat:user" ;

    /**
     * WebSocketSessionManager 초기화
     * 
     * <p>Spring 컨테이너가 빈을 생성한 후 자동으로 호출되는 초기화 메소드입니다.
     * Redis를 통한 분산 메시징을 위한 로컬 메시지 핸들러를 설정합니다.</p>
     * 
     * <h3>동작 과정:</h3>
     * <ol>
     *   <li>RedisMessageBroker에 로컬 메시지 핸들러 등록</li>
     *   <li>Redis에서 수신한 메시지를 로컬 WebSocket 세션들에게 전달</li>
     *   <li>채팅방별 토픽 필터링으로 관련 메시지만 처리</li>
     * </ol>
     * 
     * <h3>메시지 처리 흐름:</h3>
     * <pre>
     * Redis Pub/Sub → RedisMessageBroker → LocalMessageHandler → sendMessageToLocalRoom() → WebSocket 클라이언트들
     * </pre>
     * 
     * @see RedisMessageBroker#setLocalMessageHandler(RedisMessageBroker.LocalMessageHandler)
     * @see #sendMessageToLocalRoom(String, ChatMessage)
     */
    @PostConstruct
    public void initialize() {
        log.info("WebSocketSessionManager 초기화 시작");
        
        // RedisMessageBroker에 로컬 메시지 핸들러 설정
        // 다른 서버에서 발행된 메시지를 현재 서버의 WebSocket 클라이언트들에게 전달하는 역할
        redisMessageBroker.setLocalMessageHandler(new RedisMessageBroker.LocalMessageHandler() {
            /**
             * Redis에서 수신한 메시지를 로컬 WebSocket 세션들에게 전달
             * 
             * @param topic Redis 토픽명 (예: "chat:room:room123")
             * @param message 채팅 메시지 객체
             * @param messageId 메시지 고유 ID
             */
            @Override
            public void handleLocalMessage(String topic, Object message, String messageId) {
                log.info("로컬 메시지 핸들러 호출 - Topic: {}, Message: {}, MessageId: {}", topic, message, messageId);
                
                // 채팅방 토픽에서 roomId 추출 (chat:room:roomId 형태)
                if (topic != null && topic.startsWith("chat:room:")) {
                    String roomId = topic.substring("chat:room:".length());
                    
                    // ChatMessage 타입인지 확인 후 로컬 세션들에게 전송
                    if (message instanceof ChatMessage) {
                        sendMessageToLocalRoom(roomId, (ChatMessage) message);
                    } else {
                        log.warn("지원하지 않는 메시지 타입: {} - Topic: {}", message.getClass().getSimpleName(), topic);
                    }
                } else {
                    log.debug("채팅방 토픽이 아닌 메시지 무시: {}", topic);
                }
            }
            
            /**
             * 이 핸들러가 처리할 수 있는 메시지인지 판단
             * 
             * @param topic Redis 토픽명
             * @param message 메시지 객체
             * @return 채팅방 관련 토픽이면 true, 아니면 false
             */
            @Override
            public boolean canHandleLocalMessage(String topic, Object message) {
                // 채팅방 관련 토픽만 처리 (chat:room:으로 시작하는 토픽)
                boolean canHandle = topic != null && topic.startsWith("chat:room:");
                if (canHandle) {
                    log.debug("메시지 처리 가능: {}", topic);
                } else {
                    log.trace("메시지 처리 불가: {}", topic);
                }
                return canHandle;
            }
        });
        
        log.info("WebSocketSessionManager 초기화 완료 - Redis 메시지 핸들러 등록됨");
    }
    
    /**
     * 사용자 세션 추가
     * @param accountEmail 사용자 이메일
     * @param session WebSocket 세션
     */
    // 세션은 서버마다 다른값을 가지고 있고 세션끼리는 세션정보를 공유 x 그래서 메모리 값으로 관리해야 한다.
    public void addSession(String accountEmail, WebSocketSession session) {
        log.info("사용자 세션 추가 - AccountEmail: {}, SessionId: {}", accountEmail, session.getId());
        
        userSessions.computeIfAbsent(accountEmail, k -> ConcurrentHashMap.newKeySet()).add(session);
        
        log.info("사용자 {}의 활성 세션 수: {}", accountEmail, userSessions.get(accountEmail).size());
    }
    
    /**
     * 사용자 세션 제거
     * @param accountEmail 사용자 이메일
     * @param session WebSocket 세션
     */
    public void removeSession(String accountEmail, WebSocketSession session) {
        //if 카카오톡 알림같이 ,지속적으로 발생하는 이벤트에 대한 session 추적을 해야한다면 session을 유지해야한다.
        log.info("사용자 세션 제거 - AccountEmail: {}, SessionId: {}", accountEmail, session.getId());
        
        Set<WebSocketSession> sessions = userSessions.get(accountEmail);
        if (sessions != null) {
            sessions.remove(session);
            
            // 세션이 없으면 맵에서도 제거
            if (sessions.isEmpty()) {
                userSessions.remove(accountEmail);
                log.info("사용자 {}의 모든 세션이 제거됨", accountEmail);
            } else {
                log.info("사용자 {}의 활성 세션 수: {}", accountEmail, sessions.size());
            }
        }
        
        // 전체 연결된 사용자 수 계산
        int totalConnectedUsers = userSessions.values().stream()
                .mapToInt(Set::size)
                .sum();
        
        log.info("전체 연결된 사용자 수: {}", totalConnectedUsers);
        
        // 연결된 사용자가 0명이면 Redis 구독 해제 및 서버 정보 정리
        if (totalConnectedUsers == 0) {
            log.info("모든 사용자가 연결 해제됨. Redis 구독 및 서버 정보 정리 시작");
            
            // RedisMessageBroker에서 구독 중인 방 목록 가져오기
            Set<String> subscribedRooms = redisMessageBroker.getSubscribedRooms();

            if (!subscribedRooms.isEmpty()) {
                log.info("구독 중인 방 {}개 발견. 구독 해제 시작", subscribedRooms.size());
                
                // 각 방에서 구독 해제
                for (String roomId : subscribedRooms) {
                    if (roomId != null) {
                        log.info("방 {}에서 구독 해제", roomId);
                        redisMessageBroker.unsubscribeFromRoom(roomId);
                    }
                }
                
                log.info("모든 방에서 구독 해제 완료");
            } else {
                log.info("구독 중인 방이 없음");
            }
            
            // 서버 방 키 삭제 (Redis에 저장된 서버별 방 정보)
            String serverRoomsKey = serverRoomsKeyPrefix + ":" + redisMessageBroker.getInstanceId();
            Boolean deleted = redisTemplate.delete(serverRoomsKey);
            if (deleted) {
                log.info("서버 방 키 삭제 완료: {}", serverRoomsKey);
            } else {
                log.info("서버 방 키가 이미 삭제됨: {}", serverRoomsKey);
            }
            
            // 서버 사용자 키도 삭제
            String serverUsersKey = serverUsersKeyPrefix + ":" + redisMessageBroker.getInstanceId();
            Boolean userKeyDeleted = redisTemplate.delete(serverUsersKey);
            if (Boolean.TRUE.equals(userKeyDeleted)) {
                log.info("서버 사용자 키 삭제 완료: {}", serverUsersKey);
            } else {
                log.info("서버 사용자 키가 이미 삭제됨: {}", serverUsersKey);
            }
            
            log.info("Redis 구독 및 서버 정보 정리 완료");
        }
    }
    /**
     * 사용자가 채팅방에 참여
     * 
     * @param accountEmail 사용자 이메일
     * @param roomId 채팅방 ID
     */
    public void joinRoom(String accountEmail, String roomId) {
        //사용자가 채팅에 들어갈때 실행됨, redis로 구독 유무 확인하고 구독상태로 변경된다.
        String serverId = redisMessageBroker.getServerId();
        String serverRoomKey = serverRoomsKeyPrefix + ":" + serverId;
        
        // 이미 구독 중인지 확인
        Boolean wasAlreadySubscribed = redisTemplate.opsForSet().isMember(serverRoomKey, roomId);
        
        // 구독하지 않은 경우에만 구독 추가
        if (!Boolean.TRUE.equals(wasAlreadySubscribed)) {
            redisMessageBroker.subscribeToRoom(roomId);
        }
        
        // 서버별 방 정보에 방 ID 추가
        redisTemplate.opsForSet().add(serverRoomKey, roomId);
        
        log.info("사용자 {}가 방 {}에 참여 - 서버: {}, 서버방키: {}", accountEmail, roomId, serverId, serverRoomKey);
    }
    public void sendMessageToLocalRoom(String roomId, ChatMessage chatMessage) {
        try {
            String json = objectMapper.writeValueAsString(chatMessage);
            
            // 채팅방을 확인하면서, 관련된 방에 메시지를 전송
            userSessions.forEach((accountEmail, sessions) -> {
                // 해당 사용자가 채팅방 멤버인지 확인
                boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, accountEmail);
                
                if (isMember) {
                    java.util.Set<WebSocketSession> closedSessions = ConcurrentHashMap.newKeySet();
                    
                    sessions.forEach(session -> {
                        if (session.isOpen()) {
                            try {
                                session.sendMessage(new org.springframework.web.socket.TextMessage(json));
                                log.info("채팅방 {}에 메시지 전송 완료 - 사용자: {}", roomId, accountEmail);
                            } catch (Exception e) {
                                log.error("메시지 전송 실패 - 사용자: {}, 에러: {}", accountEmail, e.getMessage(), e);
                                closedSessions.add(session);
                            }
                        } else {
                            closedSessions.add(session);
                        }
                    });
                    
                    // 닫힌 세션들 제거
                    if (!closedSessions.isEmpty()) {
                        sessions.removeAll(closedSessions);
                        log.info("사용자 {}의 닫힌 세션 {}개 제거", accountEmail, closedSessions.size());
                    }
                } else {
                    log.debug("사용자 {}는 채팅방 {}의 멤버가 아님", accountEmail, roomId);
                }
            });
            
        } catch (Exception e) {
            log.error("메시지 직렬화 실패 - 채팅방: {}, 에러: {}", roomId, e.getMessage(), e);
        }
    }

    /**
     * ChatMessagePush DTO를 사용한 로컬 세션 메시지 전송 (실시간 응답성 보장)
     * 
     * @param roomId 채팅방 ID
     * @param push ChatMessagePush DTO
     */
    public void sendChatMessagePushToLocalRoom(String roomId, com.talktrip.talktrip.domain.chat.dto.response.ChatMessagePush push) {
        try {
            String json = objectMapper.writeValueAsString(push);
            
            log.info("로컬 세션에 ChatMessagePush 전송 시작 - roomId: {}, messageId: {}", roomId, push.getMessageId());
            
            // 채팅방 멤버들에게 메시지 전송
            userSessions.forEach((accountEmail, sessions) -> {
                // 해당 사용자가 채팅방 멤버인지 확인
                boolean isMember = chatRoomMemberRepository.existsByRoomIdAndAccountEmail(roomId, accountEmail);
                
                if (isMember) {
                    java.util.Set<WebSocketSession> closedSessions = ConcurrentHashMap.newKeySet();
                    
                    sessions.forEach(session -> {
                        if (session.isOpen()) {
                            try {
                                session.sendMessage(new org.springframework.web.socket.TextMessage(json));
                                log.debug("채팅방 {}에 ChatMessagePush 전송 완료 - 사용자: {}", roomId, accountEmail);
                            } catch (Exception e) {
                                log.error("ChatMessagePush 전송 실패 - 사용자: {}, 에러: {}", accountEmail, e.getMessage(), e);
                                closedSessions.add(session);
                            }
                        } else {
                            closedSessions.add(session);
                        }
                    });
                    
                    // 닫힌 세션들 제거
                    if (!closedSessions.isEmpty()) {
                        sessions.removeAll(closedSessions);
                        log.info("사용자 {}의 닫힌 세션 {}개 제거", accountEmail, closedSessions.size());
                    }
                } else {
                    log.debug("사용자 {}는 채팅방 {}의 멤버가 아님", accountEmail, roomId);
                }
            });
            
            // 기존 방식과의 호환성을 위해 SimpMessagingTemplate도 사용
            String dest = "/topic/chat/room/" + roomId;
            messagingTemplate.convertAndSend(dest, push);
            log.info("SimpMessagingTemplate으로도 전송 완료 - dest: {}", dest);
            
            log.info("로컬 세션 ChatMessagePush 전송 완료 - roomId: {}", roomId);
            
        } catch (Exception e) {
            log.error("ChatMessagePush 직렬화 실패 - 채팅방: {}, 에러: {}", roomId, e.getMessage(), e);
        }
    }

    /**
     * 특정 사용자에게 사이드바 업데이트 메시지 전송
     * 
     * @param userEmail 사용자 이메일
     * @param sidebarUpdate ChatRoomUpdateMessage DTO
     */
    public void sendSidebarUpdateToLocalUser(String userEmail, com.talktrip.talktrip.domain.chat.message.dto.ChatRoomUpdateMessage sidebarUpdate) {
        try {
            String json = objectMapper.writeValueAsString(sidebarUpdate);
            
            log.info("사용자 {}에게 사이드바 업데이트 전송 시작 - roomId: {}", userEmail, sidebarUpdate.getRoomId());
            
            // 해당 사용자의 WebSocket 세션들 조회
            java.util.Set<WebSocketSession> sessions = userSessions.get(userEmail);
            if (sessions != null && !sessions.isEmpty()) {
                java.util.Set<WebSocketSession> closedSessions = ConcurrentHashMap.newKeySet();
                
                sessions.forEach(session -> {
                    if (session.isOpen()) {
                        try {
                            session.sendMessage(new org.springframework.web.socket.TextMessage(json));
                            log.debug("사용자 {}에게 사이드바 업데이트 전송 완료", userEmail);
                        } catch (Exception e) {
                            log.error("사이드바 업데이트 전송 실패 - 사용자: {}, 에러: {}", userEmail, e.getMessage(), e);
                            closedSessions.add(session);
                        }
                    } else {
                        closedSessions.add(session);
                    }
                });
                
                // 닫힌 세션들 제거
                if (!closedSessions.isEmpty()) {
                    sessions.removeAll(closedSessions);
                    log.info("사용자 {}의 닫힌 세션 {}개 제거", userEmail, closedSessions.size());
                }
            } else {
                log.debug("사용자 {}의 활성 세션이 없음", userEmail);
            }
            
            // 기존 방식과의 호환성을 위해 SimpMessagingTemplate도 사용
            String dest = "/queue/chat/rooms";
            messagingTemplate.convertAndSendToUser(userEmail, dest, sidebarUpdate);
            log.info("SimpMessagingTemplate으로도 사이드바 업데이트 전송 완료 - user: {}, dest: {}", userEmail, dest);
            
            log.info("사용자 {}에게 사이드바 업데이트 전송 완료", userEmail);
            
        } catch (Exception e) {
            log.error("사이드바 업데이트 직렬화 실패 - 사용자: {}, 에러: {}", userEmail, e.getMessage(), e);
        }
    }

    /**
     * 사용자가 로컬 서버에서 온라인 상태인지 확인
     * 
     * <p>이 메소드는 사용자의 WebSocket 세션 상태를 실시간으로 확인하고,
     * 닫힌 세션들을 자동으로 정리하여 메모리를 최적화합니다.</p>
     * 
     * <h3>동작 과정:</h3>
     * <ol>
     *   <li>사용자의 모든 WebSocket 세션을 조회</li>
     *   <li>열려있는 세션들만 필터링하여 실제 연결 상태 확인</li>
     *   <li>닫힌 세션이 있으면 세션 목록에서 자동 제거</li>
     *   <li>모든 세션이 닫혔으면 사용자를 메모리에서 완전 제거</li>
     *   <li>실제 열린 세션이 있는지 여부를 반환</li>
     * </ol>
     * 
     * @param accountEmail 확인할 사용자의 이메일 주소
     * @return 사용자가 온라인 상태이면 true, 오프라인이거나 세션이 없으면 false
     * 
     * @see #addSession(String, WebSocketSession) 세션 추가
     * @see #removeSession(String, WebSocketSession) 세션 제거
     * @see #isUserOnline(String) 단순 온라인 상태 확인
     */
    public boolean isUserOnlineLocally(String accountEmail) {
        // 사용자의 WebSocket 세션 목록 조회
        java.util.Set<WebSocketSession> sessions = userSessions.get(accountEmail);
        if (sessions == null) {
            return false; // 세션이 없으면 오프라인
        }
        
        // 실제로 열려있는 세션들만 필터링
        java.util.Set<WebSocketSession> openSessions = sessions.stream()
                .filter(WebSocketSession::isOpen)
                .collect(Collectors.toSet());
        
        // 닫힌 세션이 있으면 정리 작업 수행
        if (openSessions.size() != sessions.size()) {
            // 닫힌 세션들을 별도로 수집
            java.util.Set<WebSocketSession> closedSessions = sessions.stream()
                    .filter(session -> !session.isOpen())
                    .collect(Collectors.toSet());
            
            // 닫힌 세션들을 원본 세션 목록에서 제거
            sessions.removeAll(closedSessions);
            
            // 모든 세션이 닫혔으면 사용자를 메모리에서 완전 제거
            if (sessions.isEmpty()) {
                userSessions.remove(accountEmail);
                log.debug("사용자 {}의 모든 세션이 닫혀 메모리에서 제거됨", accountEmail);
            }
        }
        
        // 열린 세션이 하나라도 있으면 온라인 상태
        return !openSessions.isEmpty();
    }
    
    /**
     * 사용자의 모든 세션 가져오기
     * @param accountEmail 사용자 이메일
     * @return WebSocket 세션 Set
     */
    public java.util.Set<WebSocketSession> getUserSessions(String accountEmail) {
        return userSessions.getOrDefault(accountEmail, ConcurrentHashMap.newKeySet());
    }
    
    /**
     * 사용자가 온라인인지 확인
     * @param accountEmail 사용자 이메일
     * @return 온라인 여부
     */
    public boolean isUserOnline(String accountEmail) {
        java.util.Set<WebSocketSession> sessions = userSessions.get(accountEmail);
        return sessions != null && !sessions.isEmpty();
    }
    

}
