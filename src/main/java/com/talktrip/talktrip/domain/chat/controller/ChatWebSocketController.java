package com.talktrip.talktrip.domain.chat.controller;

import com.talktrip.talktrip.domain.chat.dto.request.ChatMessageRequestDto;
import com.talktrip.talktrip.domain.chat.service.ChatService;
import com.talktrip.talktrip.global.webSocket.WebSocketErrorHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;

@RequiredArgsConstructor
@Controller
public class ChatWebSocketController {

    private static final Logger logger = LoggerFactory.getLogger(ChatWebSocketController.class);

    private final ChatService chatService;
    private final SimpMessagingTemplate messagingTemplate;
    private final WebSocketErrorHandler errorHandler;

    @MessageMapping("/chat/message")  // 클라이언트 → /app/chat/message
    public void handleMessage(ChatMessageRequestDto dto, Principal principal) {
        // 메시지 저장, 브로드캐스트, 재시도(또는 실패 처리) 등은 전적으로 와 `RedisPublisher`가 담당합니다. `ChatService`
        try {
            // 정상 처리 시 메시지 저장 및 발송
            chatService.saveAndSend(dto, principal);
        } catch (RuntimeException ex) {
            // 전용 에러 핸들러를 사용하여 에러 처리
            errorHandler.handleMessageError(
                principal, 
                dto, 
                ex, 
                "MESSAGE_PROCESSING_FAILED", 
                "메시지 처리 중 오류가 발생했습니다."
            );
        } catch (Exception ex) {
            // 예상치 못한 에러 처리
            errorHandler.handleGeneralError(
                principal, 
                ex, 
                "메시지 처리 중 예상치 못한 오류"
            );
        }
    }
}
