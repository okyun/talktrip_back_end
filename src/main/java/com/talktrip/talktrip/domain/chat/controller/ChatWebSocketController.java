package com.talktrip.talktrip.domain.chat.controller;

import com.talktrip.talktrip.domain.chat.dto.request.ChatMessageRequestDto;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import com.talktrip.talktrip.domain.chat.service.ChatService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.security.Principal;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Controller
public class ChatWebSocketController {

    private final ChatService chatService;
    private final SimpMessagingTemplate messagingTemplate;

    @MessageMapping("/chat/message")  // 클라이언트 → /app/chat/message
    public void handleMessage(ChatMessageRequestDto dto, Principal principal) {
        // 메시지 저장, 브로드캐스트, 재시도(또는 실패 처리) 등은 전적으로 와 `RedisPublisher`가 담당합니다. `ChatService`
        try {
            // 정상 처리 시 메시지 저장 및 발송
            chatService.saveAndSend(dto, principal);
        } catch (RuntimeException ex) {
            log.error("WebSocket 메시지 처리 실패: {}", ex.getMessage());

            // 클라이언트에게 실패 정보를 전달
            var errorResponse = Map.of(
                "originalMessage", dto,
                "error", ex.getMessage(),
                "errorCode", "REDIS_CONNECTION_FAILED",
                "details", "Redis 서버에 연결할 수 없습니다.",
                "failedAt", SeoulTimeUtil.nowAsString()
            );

            messagingTemplate.convertAndSendToUser(
                principal.getName(),
                "/queue/errors", // 클라이언트가 구독하는 경로
                errorResponse
            );
        }
    }
}
