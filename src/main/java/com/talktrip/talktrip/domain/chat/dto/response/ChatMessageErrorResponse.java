package com.talktrip.talktrip.domain.chat.dto.response;

import com.talktrip.talktrip.domain.chat.dto.request.ChatMessageRequestDto;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChatMessageErrorResponse {
    
    private ChatMessageRequestDto originalMessage; // 원본 메시지
    private String error; // 에러 메시지
    private String errorCode; // 에러 코드
    private LocalDateTime failedAt; // 실패 시간
    private String details; // 상세 에러 정보
    
    public static ChatMessageErrorResponse of(ChatMessageRequestDto originalMessage, String error) {
        return ChatMessageErrorResponse.builder()
                .originalMessage(originalMessage)
                .error(error)
                .errorCode("MESSAGE_SEND_FAILED")
                .failedAt(SeoulTimeUtil.now())
                .details("메시지 전송 중 오류가 발생했습니다.")
                .build();
    }
    
    public static ChatMessageErrorResponse of(ChatMessageRequestDto originalMessage, String error, String errorCode, String details) {
        return ChatMessageErrorResponse.builder()
                .originalMessage(originalMessage)
                .error(error)
                .errorCode(errorCode)
                .failedAt(SeoulTimeUtil.now())
                .details(details)
                .build();
    }
}
