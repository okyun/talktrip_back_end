package com.talktrip.talktrip.domain.chat.repository;

import com.talktrip.talktrip.domain.chat.entity.ChatRoom;
import com.talktrip.talktrip.domain.chat.entity.ChatRoomAccount;
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
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Import({QuerydslConfig.class, ChatRoomMemberRepositoryTest.AuditingTestConfig.class})
@ActiveProfiles("test")
class ChatRoomMemberRepositoryTest {

    @Autowired
    private ChatRoomMemberRepository chatRoomMemberRepository;

    @PersistenceContext
    private EntityManager entityManager;

    private String testRoomId1;
    private String  testRoomId2;
    private String testUser1Email;
    private String testUser2Email;
    private String testUser3Email;
    private LocalDateTime baseTime;

    @BeforeEach
    void setUp() {
        testRoomId1 = "TEST_ROOM_1_" + UUID.randomUUID();
        testRoomId2 = "TEST_ROOM_2_" + UUID.randomUUID();
        testUser1Email = "user1@test.com";
        testUser2Email = "user2@test.com";
        testUser3Email = "user3@test.com";
        baseTime = LocalDateTime.of(2024, 1, 1, 10, 0, 0);

        // ChatRoom 생성
        ChatRoom chatRoom1 = ChatRoom.builder()
                .roomId(testRoomId1)
                .title("테스트 채팅방 1")
                
                .productId(1)
                .roomType(RoomType.DIRECT)
                .build();

        ChatRoom chatRoom2 = ChatRoom.builder()
                .roomId(testRoomId2)
                .title("테스트 채팅방 2")

                .productId(2)
                .roomType(RoomType.GROUP)
                .build();

        entityManager.persist(chatRoom1);
        entityManager.persist(chatRoom2);

        // ChatRoomAccount 생성
        ChatRoomAccount roomAccount1 = ChatRoomAccount.create(testRoomId1, testUser1Email);
        ChatRoomAccount roomAccount2 = ChatRoomAccount.create(testRoomId1, testUser2Email);
        ChatRoomAccount roomAccount3 = ChatRoomAccount.create(testRoomId2, testUser1Email);

        entityManager.persist(roomAccount1);
        entityManager.persist(roomAccount2);
        entityManager.persist(roomAccount3);

        entityManager.clear();
    }

    @Test
    @DisplayName("Room ID 및 Member ID로 마지막 읽은 시간 업데이트")
    void updateLastReadTime() {
        // Given
        LocalDateTime beforeUpdate = getLastReadTime(testRoomId1, testUser1Email);

        // When
        int updatedCount = chatRoomMemberRepository.updateLastReadTime(testRoomId1, testUser1Email);
        entityManager.flush();
        entityManager.clear();

        // Then
        assertThat(updatedCount).isEqualTo(1);

        LocalDateTime afterUpdate = getLastReadTime(testRoomId1, testUser1Email);
        assertThat(afterUpdate).isNotNull().isAfter(beforeUpdate != null ? beforeUpdate : baseTime.minusHours(1));
    }

    @Test
    @DisplayName("구매자와 판매자가 한 채팅방에 있는지 확인")
    void findRoomIdByBuyerIdAndSellerId() {
        // When
        Optional<String> result = chatRoomMemberRepository.findRoomIdByBuyerIdAndSellerId(testUser1Email, testUser2Email);

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(testRoomId1);
    }

    @Test
    @DisplayName("사용자 목록 조회")
    void findParticipantEmails() {
        // When
        var participants = chatRoomMemberRepository.findParticipantEmails(testRoomId1);

        // Then
        assertThat(participants)
                .isNotEmpty()
                .containsExactlyInAnyOrder(testUser1Email, testUser2Email);
    }

    @Test
    @DisplayName("Room ID로 모든 사용자 삭제 플래그 초기화")
    void resetIsDelByRoomId() {
        // Given
        chatRoomMemberRepository.updateIsDelByMemberIdAndRoomId(testUser1Email, testRoomId1, 1);

        // When
        chatRoomMemberRepository.resetIsDelByRoomId(testRoomId1);
        entityManager.flush();
        entityManager.clear();

        // Then
        var roomMembers = chatRoomMemberRepository.findAllAccountEmailsByRoomId(testRoomId1);
        assertThat(roomMembers).allMatch(acc -> acc.getIsDel() == 0);
    }

    private LocalDateTime getLastReadTime(String roomId, String email) {
        return chatRoomMemberRepository.findMyLastReadAt(roomId, email).orElse(null);
    }

    @TestConfiguration
    @EnableJpaAuditing
    static class AuditingTestConfig {
    }
}