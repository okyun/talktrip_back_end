package com.talktrip.talktrip.global.webSocket;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.net.URI;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class WebSocketHandshakeInterceptor implements HandshakeInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketHandshakeInterceptor.class);

    // 이 인터셉터를 적용할 WS 엔드포인트 prefix (예: WebSocketConfig에서 등록한 경로)
    private static final String WS_PATH_PREFIX = "/ws/websocket";

    private boolean isTargetWebSocketEndpoint(URI uri) {
        if (uri == null) return false;
        String path = uri.getPath();
        return path != null && path.contains(WS_PATH_PREFIX);
    }

    @Override
    public boolean beforeHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Map<String, Object> attributes
    ) throws Exception {

        // 대상 경로가 아니면 아무 것도 하지 않음
        if (!isTargetWebSocketEndpoint(request.getURI())) {
            return true;
        }

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null) {
            attributes.put("authentication", authentication);
        }

        // 필요 시 디버그 로그 (운영에선 debug 레벨 권장)
        if (logger.isDebugEnabled()) {
            logger.debug("beforeHandshake: path={}, principalPresent={}",
                    request.getURI().getPath(),
                    authentication != null);
        }

        return true;
    }

    @Override
    public void afterHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Exception exception
    ) {
        // 대상 경로가 아니면 로그 생략
        if (!isTargetWebSocketEndpoint(request.getURI())) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("afterHandshake 완료. path={}, error={}",
                    request.getURI().getPath(),
                    exception == null ? "none" : exception.getClass().getSimpleName());
        }
        if (exception != null){
            logger.error("websocket HandShakerInterception exception:",exception);

        }
    }
}
