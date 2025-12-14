package com.talktrip.talktrip.global.webSocket;

import com.talktrip.talktrip.domain.chat.dto.response.ChatMessageErrorResponse;
import com.talktrip.talktrip.domain.chat.dto.request.ChatMessageRequestDto;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.Map;

/**
 * STOMP WebSocket 에러 처리 핸들러
 * 
 * Raw WebSocket의 handleTransportError와 동일한 역할을 수행
 * - 연결 에러 처리
 * - 메시지 전송 에러 처리
 * - 클라이언트에게 에러 정보 전달
 */
@Component
@RequiredArgsConstructor
public class WebSocketErrorHandler {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketErrorHandler.class);

    private final SimpMessagingTemplate messagingTemplate;

    /**
     * 메시지 처리 에러 발생 시 클라이언트에게 에러 정보 전달
     * 
     * @param principal 사용자 정보
     * @param originalMessage 원본 메시지
     * @param error 에러 객체
     * @param errorCode 에러 코드
     * @param details 에러 상세 정보
     */
    public void handleMessageError(Principal principal, 
                                 ChatMessageRequestDto originalMessage, 
                                 Throwable error, 
                                 String errorCode, 
                                 String details) {
        try {
            logger.error("WebSocket 메시지 처리 에러 - 사용자: {}, 에러: {}", 
                     principal != null ? principal.getName() : "unknown", error.getMessage(), error);

            // 에러 응답 생성
            ChatMessageErrorResponse errorResponse = ChatMessageErrorResponse.of(
                originalMessage, 
                error.getMessage(), 
                errorCode, 
                details
            );

            // 클라이언트에게 에러 정보 전달
            if (principal != null) {
                messagingTemplate.convertAndSendToUser(
                    principal.getName(),
                    "/queue/errors", // 클라이언트가 구독하는 경로
                    errorResponse
                );
            } else {
                // 인증되지 않은 사용자의 경우 전체 브로드캐스트
                messagingTemplate.convertAndSend("/topic/errors", errorResponse);
            }

        } catch (Exception e) {
            logger.error("에러 핸들러에서 예외 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * 연결 에러 발생 시 처리
     * 
     * @param sessionId 세션 ID
     * @param error 에러 객체
     */
    public void handleConnectionError(String sessionId, Throwable error) {
        try {
            logger.error("WebSocket 연결 에러 - 세션: {}, 에러: {}", sessionId, error.getMessage(), error);

            // 연결 에러 응답 생성
            String errorType = "CONNECTION_ERROR";
            String message = "WebSocket 연결에 문제가 발생했습니다.";
            String errorMessage = error.getMessage();
            String failedAt = SeoulTimeUtil.nowAsString();
            
            var errorResponse = Map.of(
                "errorType", errorType,
                "message", message,
                "sessionId", sessionId,
                "errorMessage", errorMessage,
                "failedAt", failedAt
            );

            // 전체 브로드캐스트로 연결 에러 알림
            messagingTemplate.convertAndSend("/topic/connection-errors", errorResponse);

        } catch (Exception e) {
            logger.error("연결 에러 핸들러에서 예외 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * 전송 에러 발생 시 처리
     * 
     * @param principal 사용자 정보
     * @param destination 목적지
     * @param error 에러 객체
     */
    public void handleSendError(Principal principal, String destination, Throwable error) {
        try {
            logger.error("WebSocket 메시지 전송 에러 - 사용자: {}, 목적지: {}, 에러: {}", 
                     principal != null ? principal.getName() : "unknown", destination, error.getMessage(), error);

            // 전송 에러 응답 생성
            String errorType = "SEND_ERROR";
            String message = "메시지 전송에 실패했습니다.";
            String errorMessage = error.getMessage();
            String failedAt = SeoulTimeUtil.nowAsString();
            
            var errorResponse = Map.of(
                "errorType", errorType,
                "message", message,
                "destination", destination,
                "errorMessage", errorMessage,
                "failedAt", failedAt
            );

            // 사용자에게 전송 에러 알림
            if (principal != null) {
                messagingTemplate.convertAndSendToUser(
                    principal.getName(),
                    "/queue/send-errors",
                    errorResponse
                );
            }

        } catch (Exception e) {
            logger.error("전송 에러 핸들러에서 예외 발생: {}", e.getMessage(), e);
        }
    }

    /**
     * 일반적인 WebSocket 에러 처리
     * 
     * @param principal 사용자 정보
     * @param error 에러 객체
     * @param context 에러 발생 컨텍스트
     */
    public void handleGeneralError(Principal principal, Throwable error, String context) {
        try {
            logger.error("WebSocket 일반 에러 - 사용자: {}, 컨텍스트: {}, 에러: {}", 
                     principal != null ? principal.getName() : "unknown", context, error.getMessage(), error);

            // 일반 에러 응답 생성
            String errorType = "GENERAL_ERROR";
            String message = "WebSocket 처리 중 오류가 발생했습니다.";
            String errorMessage = error.getMessage();
            String failedAt = SeoulTimeUtil.nowAsString();
            
            var errorResponse = Map.of(
                "errorType", errorType,
                "message", message,
                "context", context,
                "errorMessage", errorMessage,
                "failedAt", failedAt
            );

            // 사용자에게 에러 알림
            if (principal != null) {
                messagingTemplate.convertAndSendToUser(
                    principal.getName(),
                    "/queue/general-errors",
                    errorResponse
                );
            }

        } catch (Exception e) {
            logger.error("일반 에러 핸들러에서 예외 발생: {}", e.getMessage(), e);
        }
    }
}
