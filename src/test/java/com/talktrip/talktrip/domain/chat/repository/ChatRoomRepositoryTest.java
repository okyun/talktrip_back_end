package com.talktrip.talktrip.domain.chat.repository;

import com.talktrip.talktrip.domain.chat.entity.ChatRoom;
import com.talktrip.talktrip.domain.chat.enums.RoomType;
import com.talktrip.talktrip.global.config.QuerydslConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.test.context.ActiveProfiles;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Import({QuerydslConfig.class, ChatRoomRepositoryTest.AuditingTestConfig.class}) // Auditing 설정 포함
@ActiveProfiles("test")
class ChatRoomRepositoryTest {

    @Autowired
    private ChatRoomRepository chatRoomRepository;

    @PersistenceContext
    private EntityManager entityManager;

    private String roomId1;
    private String roomId2;

    @BeforeEach
    void setUp() {
        roomId1 = "ROOM_1_" + UUID.randomUUID();
        roomId2 = "ROOM_2_" + UUID.randomUUID();

        ChatRoom chatRoom1 = ChatRoom.builder()
                .roomId(roomId1)
                .title("Chat Room 1")
                .productId(1)
                .roomType(RoomType.DIRECT)
                .build();

        ChatRoom chatRoom2 = ChatRoom.builder()
                .roomId(roomId2)
                .title("Chat Room 2")
                .productId(2)
                .roomType(RoomType.GROUP)
                .build();

        // 엔티티를 영속화 및 DB에 저장
        entityManager.persist(chatRoom1);
        entityManager.persist(chatRoom2);
        entityManager.flush();
        entityManager.clear();
    }

    @Test
    @DisplayName("특정 Room ID로 ChatRoom 조회")
    void testFindRoom() {
        Optional<ChatRoom> result = chatRoomRepository.findRoom(roomId1);

        assertThat(result).isPresent();
        ChatRoom chatRoom = result.get();
        assertThat(chatRoom.getRoomId()).isEqualTo(roomId1);
        assertThat(chatRoom.getCreatedAt()).isNotNull(); // createdAt 확인
    }

    @TestConfiguration
    @EnableJpaAuditing // 테스트에서 Auditing 활성화
    static class AuditingTestConfig {
    }
}
