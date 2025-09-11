package com.talktrip.talktrip.global.webSocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class WebSocketEventsLogger {

    private final String instanceId;

    @EventListener
    public void onConnect(SessionConnectEvent e) {
        StompHeaderAccessor acc = StompHeaderAccessor.wrap(e.getMessage());
        log.info("[{}][WS] CONNECT sessionId={}, user={}", instanceId, acc.getSessionId(), acc.getUser());
    }

    @EventListener
    public void onDisconnect(SessionDisconnectEvent e) {
        StompHeaderAccessor acc = StompHeaderAccessor.wrap(e.getMessage());
        log.info("[{}][WS] DISCONNECT sessionId={}, closeStatus={}", instanceId, acc.getSessionId(), e.getCloseStatus());
    }

    @EventListener
    public void onMessage(StompHeaderAccessor acc) {
        log.info("[{}][WS] MESSAGE sessionId={}, user={}", instanceId, acc.getSessionId(), acc.getUser());
    }
//    - 인스턴스 ID 태깅: 각 WAS가 자신만의 instanceId(예: hostname:port 또는 랜덤 UUID)를 가지게 하고, 주요 지점(시작, Redis 수신, WebSocket 전송, STOMP 연결/해제 이벤트)에 로그에 찍기.
//- Actuator info 노출: /actuator/info에 instanceId를 노출해 각 포트(8081/8082/8083)에서 서로 다른 값이 보이는지 확인.
//            - 이벤트 리스너: STOMP SessionConnect/Disconnect를 리스닝해 “어느 인스턴스에 세션이 붙었는지”를 로그로 확인.
//- 테스트용 API: /api/test/publish로 Redis Pub 이벤트를 쏘고, 각 인스턴스 로그에 수신 로그가 찍히는지 확인.
//
//

}
