package com.talktrip.talktrip.domain.chat.dto.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@NoArgsConstructor
public class ChatMessageRequestDto {
    private String roomId;
    private String accountEmail;
    private String message;
    //private String receiverAccountEmail;

    @JsonCreator
    public ChatMessageRequestDto(
            @JsonProperty("roomId") String roomId,
            @JsonProperty("accountEmail") String accountEmail,
            @JsonProperty("message") String message

            ) {
        this.roomId = roomId;
        this.accountEmail = accountEmail;
        this.message = message;

    }

    public ChatMessage toEntity(String accountEmail, Long sequenceNumber) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String messageId = "mgs" + uuid.substring(0, 7);

        return new ChatMessage(
                messageId,
                this.roomId,
                accountEmail,
                this.message,
                sequenceNumber,
                SeoulTimeUtil.now()
        );
    }
}