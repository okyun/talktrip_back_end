package com.talktrip.talktrip.global.webSocket;

import com.talktrip.talktrip.global.webSocket.stomp.JwtStompChannelInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;

@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final WebSocketHandshakeInterceptor handshakeInterceptor;
    
    // JwtStompChannelInterceptor를 선택적 의존성으로 변경
    @Autowired(required = false)
    private JwtStompChannelInterceptor jwtStompChannelInterceptor;

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        // jwtStompChannelInterceptor가 null이 아닐 때만 등록
        if (jwtStompChannelInterceptor != null) {
            registration.interceptors(jwtStompChannelInterceptor);
        }
    }
    
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")// localhost:8080/ws
                .setAllowedOrigins("http://localhost:5173") // ✅ 프론트 주소들 추가
                .addInterceptors(handshakeInterceptor) // ✅ 꼭 넣기
                .withSockJS()
                .setWebSocketEnabled(true)
                .setHeartbeatTime(25000)
                .setDisconnectDelay(5000)
                .setHttpMessageCacheSize(1000)
                .setStreamBytesLimit(2 * 1024 * 1024)  // 2MB로 증가
                .setHttpMessageCacheSize(1000)
                .setSessionCookieNeeded(false);
        
        // 네이티브 WebSocket 엔드포인트 추가
        registry.addEndpoint("/ws/websocket")
                .setAllowedOrigins("http://localhost:5173")
                .addInterceptors(handshakeInterceptor);
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // 브로커 목적지: topic(그룹), queue(포인트투포인트 용어) 둘 다 활성화
        registry.enableSimpleBroker("/topic", "/queue");// 구독 url,서버 → 클라이언트// 브로커(바로 브로드캐스트)
        registry.setApplicationDestinationPrefixes("/app");// prefix 정의, 클라이언트 → 서버 // 컨트롤러로 가는 입구

        // kafka, rabbitmq 여기서 설정
    }

    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
        // 큰 메시지 처리 정책 설정
        registration.setMessageSizeLimit(1024 * 1024);        // 1MB - 단일 메시지 최대 크기
        registration.setSendBufferSizeLimit(2 * 1024 * 1024); // 2MB - 전송 버퍼 크기
        registration.setSendTimeLimit(30000);                 // 30초 - 전송 시간 제한
        registration.setTimeToFirstMessage(30000);            // 30초 - 첫 메시지까지 대기 시간
        
        // 큰 메시지 자동 분할 처리 (Raw WebSocket의 supportsPartialMessages(true)와 동일)
        // STOMP는 자동으로 큰 메시지를 처리하므로 별도 DecoratorFactory 불필요
    }

}
