package com.talktrip.talktrip.domain.chat.entity;

import com.talktrip.talktrip.domain.chat.enums.RoomType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChatRoomTest {

    @Test
    @DisplayName("ChatRoom Builder를 통해 객체를 정상적으로 생성한다")
    void createChatRoom() {
        // Given
        String roomId = "ROOM_001";
        String title = "Test Chat Room";
        int notReadMessageCount = 10;
        int productId = 123;
        RoomType roomType = RoomType.DIRECT;

        // When
        ChatRoom chatRoom = ChatRoom.builder()
                .roomId(roomId)
                .title(title)

                .productId(productId)
                .roomType(roomType)
                .build();

        // Then
        assertThat(chatRoom.getRoomId()).isEqualTo(roomId);
        assertThat(chatRoom.getTitle()).isEqualTo(title);

        assertThat(chatRoom.getProductId()).isEqualTo(productId);
        assertThat(chatRoom.getRoomType()).isEqualTo(roomType);
    }


}