package com.talktrip.talktrip.domain.chat.dto.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;

import java.time.LocalDateTime;

public record ChatMemberRoomWithMessageDto(
    String messageId,       // 메시지 PK (커서용 + 정렬)
    String roomId,          // 방 ID
    String accountEmail,    // 보낸 사람 식별자
    String message,         // 메시지 본문
    Long sequenceNumber,    // 메시지 순서 번호
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    LocalDateTime createdAt, // 생성 시각
    String senderName
){
    public static ChatMemberRoomWithMessageDto from(ChatMessage message) {
        return new ChatMemberRoomWithMessageDto(
                message.getMessageId(),
                message.getRoomId(),
                message.getAccountEmail(),
                message.getMessage(),
                message.getSequenceNumber(),
                message.getCreatedAt(),
                message.getSenderRef() != null ? message.getSenderRef().getName() : null
        );
    }
}
