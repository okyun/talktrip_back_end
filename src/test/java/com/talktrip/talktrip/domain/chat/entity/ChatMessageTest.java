package com.talktrip.talktrip.domain.chat.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class ChatMessageTest {

    @Test
    @DisplayName("ChatMessage Builder를 사용해 객체를 성공적으로 생성한다")
    void createChatMessage() {
        // Given
        String messageId = "MSG_001";
        String roomId = "ROOM_001";
        String accountEmail = "user@test.com";
        String messageContent = "Hello, World!";
        LocalDateTime createdAt = LocalDateTime.now();

        // When
        ChatMessage chatMessage = new ChatMessage(messageId, roomId, accountEmail, messageContent, 1L, createdAt);

        // Then
        assertThat(chatMessage.getMessageId()).isEqualTo(messageId);
        assertThat(chatMessage.getRoomId()).isEqualTo(roomId);
        assertThat(chatMessage.getAccountEmail()).isEqualTo(accountEmail);
        assertThat(chatMessage.getMessage()).isEqualTo(messageContent);
        assertThat(chatMessage.getCreatedAt()).isEqualTo(createdAt);
    }

    @Test
    @DisplayName("roomRef와 senderRef가 null 처리되어 생성된다")
    void nullReferences() {
        // Given
        String messageId = "MSG_002";
        String roomId = "ROOM_002";
        String accountEmail = "user@test.com";
        String messageContent = "Null reference test";
        LocalDateTime createdAt = LocalDateTime.now();

        // When
        ChatMessage chatMessage = new ChatMessage(messageId, roomId, accountEmail, messageContent, 1L, createdAt);

        // Then
        assertThat(chatMessage.getRoomRef()).isNull();
        assertThat(chatMessage.getSenderRef()).isNull();
    }
}