package com.talktrip.talktrip.domain.chat.repository;

import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import com.talktrip.talktrip.domain.chat.entity.ChatRoom;
import com.talktrip.talktrip.domain.chat.entity.ChatRoomAccount;
import com.talktrip.talktrip.domain.chat.enums.RoomType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
// 정확성: 각 메소드가 올바른 값을 반환하는지 검증
// 경계 조건: null, 빈 값, 존재하지 않는 데이터 처리
// 성능: 합리적인 범위 내의 결과값
// 일관성: 여러 조건에서 일관된 동작
// 정렬: 시간순, ID순 정렬이 올바른지 확인
@DataJpaTest
@ActiveProfiles("test")
class ChatMessageRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private ChatMessageRepository chatMessageRepository;

    private String testRoomId;
    private String testUser1Email;
    private String testUser2Email;
    private LocalDateTime baseTime;

    @BeforeEach
    void setUp() {
        testRoomId = "TEST_ROOM_" + UUID.randomUUID().toString().substring(0, 8);
        testUser1Email = "user1@test.com";
        testUser2Email = "user2@test.com";
        baseTime = LocalDateTime.now();

        // ChatRoom 생성
        ChatRoom chatRoom = ChatRoom.builder()
                .roomId(testRoomId)
                .title("테스트 채팅방")

                .productId(1)
                .roomType(RoomType.DIRECT)
                .build();
        entityManager.persistAndFlush(chatRoom);

        // ChatRoomAccount 생성 (두 명의 사용자)
        ChatRoomAccount roomAccount1 = ChatRoomAccount.create(testRoomId, testUser1Email);
        ChatRoomAccount roomAccount2 = ChatRoomAccount.create(testRoomId, testUser2Email);
        
        entityManager.persistAndFlush(roomAccount1);
        entityManager.persistAndFlush(roomAccount2);

        entityManager.clear();
    }

    @Test
    @DisplayName("특정 방의 읽지 않은 메시지 수를 정확히 계산한다")
    void countUnreadMessagesByRoomId() {
        // Given
        // user1이 마지막으로 읽은 메시지 생성
        ChatMessage readMessage = createChatMessage("MSG_READ", testUser2Email, "읽은 메시지", baseTime);
        entityManager.persistAndFlush(readMessage);

        // user1의 마지막 읽은 시간을 설정
        //updateLastReadTime(testUser1Email, baseTime);

        // 읽지 않은 메시지들 생성 (마지막 읽은 시간 이후)
        ChatMessage unreadMessage1 = createChatMessage("MSG_UNREAD_1", testUser2Email, "읽지 않은 메시지 1", baseTime.plusMinutes(1));
        ChatMessage unreadMessage2 = createChatMessage("MSG_UNREAD_2", testUser2Email, "읽지 않은 메시지 2", baseTime.plusMinutes(2));

        entityManager.persistAndFlush(unreadMessage1);
        entityManager.persistAndFlush(unreadMessage2);
        entityManager.flush();

        // When
        int unreadCount = chatMessageRepository.countUnreadMessagesByRoomId(testRoomId, testUser1Email);

        // Then
        assertThat(unreadCount)
                .as("user1이 읽지 않은 메시지 수는 2개여야 함")
                .isEqualTo(2)
                .isPositive()
                .isLessThanOrEqualTo(10); // 합리적인 범위 체크
    }

    @Test
    @DisplayName("사용자가 읽지 않은 메시지가 있는 채팅방 수를 정확히 계산한다")
    void countUnreadMessagesRooms() {
        // Given
        String anotherRoomId = createAnotherRoom();
        
        // 첫 번째 방에 읽지 않은 메시지 추가
        ChatMessage message1 = createChatMessage("MSG_1", testUser2Email, "메시지 1", baseTime);
        entityManager.persistAndFlush(message1);

        // 두 번째 방에 읽지 않은 메시지 추가
        ChatMessage message2 = createChatMessage("MSG_2", testUser2Email, "메시지 2", baseTime, anotherRoomId);
        entityManager.persistAndFlush(message2);

        entityManager.flush();

        // When
        int unreadRoomCount = chatMessageRepository.countUnreadMessagesRooms(testUser1Email);

        // Then
        assertThat(unreadRoomCount)
                .as("읽지 않은 메시지가 있는 채팅방 수")
                .isEqualTo(2)
                .isBetween(1, 10); // 합리적인 범위
    }

    @Test
    @DisplayName("사용자의 전체 읽지 않은 메시지 수를 정확히 계산한다")
    void countUnreadMessages() {
        // Given
        String anotherRoomId = createAnotherRoom();
        
        // 첫 번째 방에 메시지 3개 추가
        List<ChatMessage> room1Messages = List.of(
                createChatMessage("MSG_ROOM1_1", testUser2Email, "방1 메시지 1", baseTime.plusMinutes(1)),
                createChatMessage("MSG_ROOM1_2", testUser2Email, "방1 메시지 2", baseTime.plusMinutes(2)),
                createChatMessage("MSG_ROOM1_3", testUser2Email, "방1 메시지 3", baseTime.plusMinutes(3))
        );
        room1Messages.forEach(entityManager::persistAndFlush);

        // 두 번째 방에 메시지 2개 추가
        List<ChatMessage> room2Messages = List.of(
                createChatMessage("MSG_ROOM2_1", testUser2Email, "방2 메시지 1", baseTime.plusMinutes(1), anotherRoomId),
                createChatMessage("MSG_ROOM2_2", testUser2Email, "방2 메시지 2", baseTime.plusMinutes(2), anotherRoomId)
        );
        room2Messages.forEach(entityManager::persistAndFlush);

        entityManager.flush();

        // When
        int totalUnreadCount = chatMessageRepository.countUnreadMessages(testUser1Email);

        // Then
        assertThat(totalUnreadCount)
                .as("전체 읽지 않은 메시지 수")
                .isEqualTo(5)
                .isGreaterThan(0)
                .satisfies(count -> {
                    assertThat(count).as("방1 메시지 수 + 방2 메시지 수와 일치").isEqualTo(3 + 2);
                });
    }

    @Test
    @DisplayName("특정 방에서 특정 사용자의 읽지 않은 메시지 수를 정확히 계산한다")
    void countUnreadMessagesByRoomIdAndMemberId() {
        // Given
        LocalDateTime lastReadTime = baseTime;
        //updateLastReadTime(testUser1Email, lastReadTime);

        // user2가 보낸 메시지들 (user1이 읽지 않음)
        List<ChatMessage> unreadMessages = List.of(
                createChatMessage("MSG_UNREAD_1", testUser2Email, "읽지 않은 메시지 1", lastReadTime.plusMinutes(1)),
                createChatMessage("MSG_UNREAD_2", testUser2Email, "읽지 않은 메시지 2", lastReadTime.plusMinutes(2))
        );
        unreadMessages.forEach(entityManager::persistAndFlush);

        // user1이 보낸 메시지 (자신의 메시지는 읽지 않은 것으로 카운트되지 않음)
        ChatMessage myMessage = createChatMessage("MSG_MY", testUser1Email, "내가 보낸 메시지", lastReadTime.plusMinutes(3));
        entityManager.persistAndFlush(myMessage);
        
        entityManager.flush();

        // When
        int unreadCount = chatMessageRepository.countUnreadMessagesByRoomIdAndMemberId(testRoomId, testUser1Email);

        // Then
        assertThat(unreadCount)
                .as("상대방이 보낸 읽지 않은 메시지만 카운트")
                .isEqualTo(2)
                .satisfies(count -> {
                    assertThat(count).as("자신이 보낸 메시지는 제외").isLessThan(3);
                    assertThat(count).as("상대방 메시지만 포함").isEqualTo(unreadMessages.size());
                });
    }

    @Test
    @DisplayName("특정 방에서 다른 사용자의 이메일을 정확히 조회한다")
    void getOtherMemberIdByRoomIdandUserId() {
        // When
        String otherMemberEmail = chatMessageRepository.getOtherMemberIdByRoomIdandUserId(testUser1Email, testRoomId);

        // Then
        assertThat(otherMemberEmail)
                .as("다른 사용자의 이메일")
                .isNotNull()
                .isNotEmpty()
                .isEqualTo(testUser2Email)
                .isNotEqualTo(testUser1Email)
                .contains("@")
                .endsWith(".com");
    }

    @Test
    @DisplayName("첫 페이지 메시지를 최신순으로 정확히 조회한다")
    void findFirstPage() {
        // Given
        List<ChatMessage> messages = List.of(
                createChatMessage("MSG_01", testUser1Email, "메시지 1", baseTime.plusMinutes(1)),
                createChatMessage("MSG_02", testUser2Email, "메시지 2", baseTime.plusMinutes(2)),
                createChatMessage("MSG_03", testUser1Email, "메시지 3", baseTime.plusMinutes(3)),
                createChatMessage("MSG_04", testUser2Email, "메시지 4", baseTime.plusMinutes(4)),
                createChatMessage("MSG_05", testUser1Email, "메시지 5", baseTime.plusMinutes(5))
        );
        messages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // When
        PageRequest pageRequest = PageRequest.of(0, 3);
        List<ChatMessage> result = chatMessageRepository.findFirstPage(testRoomId, pageRequest);

        // Then
        assertThat(result)
                .as("첫 페이지 메시지 조회 결과")
                .isNotNull()
                .hasSize(3)
                .extracting(ChatMessage::getMessage)
                .containsExactly("메시지 5", "메시지 4", "메시지 3") // 최신순
                .doesNotContain("메시지 1", "메시지 2");

        // 추가 검증: 시간 순서 확인
        assertThat(result)
                .as("메시지가 최신순으로 정렬됨")
                .isSortedAccordingTo((m1, m2) -> m2.getCreatedAt().compareTo(m1.getCreatedAt()));

        // 첫 번째 메시지가 가장 최신인지 확인
        assertThat(result.get(0))
                .as("첫 번째 메시지는 가장 최신")
                .satisfies(msg -> {
                    assertThat(msg.getMessage()).isEqualTo("메시지 5");
                    assertThat(msg.getCreatedAt()).isEqualTo(baseTime.plusMinutes(5));
                });
    }

    @Test
    @DisplayName("커서 이전 메시지를 정확히 조회한다")
    void findSliceBefore() {
        // Given
        List<ChatMessage> allMessages = List.of(
                createChatMessage("MSG_001", testUser1Email, "메시지 1", baseTime),
                createChatMessage("MSG_002", testUser2Email, "메시지 2", baseTime.plusMinutes(1)),
                createChatMessage("MSG_003", testUser1Email, "메시지 3", baseTime.plusMinutes(2)),
                createChatMessage("MSG_004", testUser2Email, "메시지 4", baseTime.plusMinutes(3))
        );
        allMessages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        ChatMessage cursorMessage = allMessages.get(2); // MSG_003

        // When
        PageRequest pageRequest = PageRequest.of(0, 2);
        List<ChatMessage> result = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest
        );

        // Then
        assertThat(result)
                .as("커서 이전 메시지들")
                .isNotNull()
                .hasSize(2)
                .extracting(ChatMessage::getMessageId)
                .containsExactly("MSG_002", "MSG_001") // 최신순으로 정렬
                .doesNotContain("MSG_003", "MSG_004"); // 커서 메시지와 이후 메시지는 제외

        // 모든 메시지가 커서보다 이전 시간인지 확인
        assertThat(result)
                .as("모든 메시지가 커서보다 이전")
                .allSatisfy(msg -> 
                    assertThat(msg.getCreatedAt()).isBefore(cursorMessage.getCreatedAt())
                );
    }

    @Test
    @DisplayName("존재하지 않는 방의 읽지 않은 메시지 수는 0이다")
    void countUnreadMessagesByRoomId_NonExistentRoom() {
        // Given
        String nonExistentRoomId = "NON_EXISTENT_ROOM";

        // When
        int unreadCount = chatMessageRepository.countUnreadMessagesByRoomId(nonExistentRoomId, testUser1Email);

        // Then
        assertThat(unreadCount)
                .as("존재하지 않는 방의 읽지 않은 메시지 수")
                .isZero()
                .isNotNegative();
    }

    @Test
    @DisplayName("존재하지 않는 사용자의 읽지 않은 메시지 수는 0이다")
    void countUnreadMessages_NonExistentUser() {
        // Given
        String nonExistentUser = "nonexistent@test.com";

        // When
        int unreadCount = chatMessageRepository.countUnreadMessages(nonExistentUser);

        // Then
        assertThat(unreadCount)
                .as("존재하지 않는 사용자의 읽지 않은 메시지 수")
                .isZero()
                .isNotNegative();
    }

    @Test
    @DisplayName("빈 방에서 첫 페이지 조회 시 빈 리스트를 반환한다")
    void findFirstPage_EmptyRoom() {
        // Given
        String emptyRoomId = createEmptyRoom();

        // When
        PageRequest pageRequest = PageRequest.of(0, 10);
        List<ChatMessage> result = chatMessageRepository.findFirstPage(emptyRoomId, pageRequest);

        // Then
        assertThat(result)
                .as("빈 방의 메시지 조회 결과")
                .isNotNull()
                .isEmpty();
        
        assertThat(result)
                .hasSize(0);
    }

    @Test
    @DisplayName("같은 시간에 생성된 메시지들은 messageId로 정렬된다")
    void findFirstPage_SameCreatedAt() {
        // Given
        LocalDateTime sameTime = baseTime;
        
        List<ChatMessage> messagesWithSameTime = List.of(
                createChatMessage("MSG_A", testUser1Email, "메시지 A", sameTime),
                createChatMessage("MSG_B", testUser2Email, "메시지 B", sameTime),
                createChatMessage("MSG_C", testUser1Email, "메시지 C", sameTime)
        );
        messagesWithSameTime.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // When
        PageRequest pageRequest = PageRequest.of(0, 3);
        List<ChatMessage> result = chatMessageRepository.findFirstPage(testRoomId, pageRequest);

        // Then
        assertThat(result)
                .as("같은 시간 메시지들의 정렬 결과")
                .isNotNull()
                .hasSize(3)
                .extracting(ChatMessage::getMessageId)
                .containsExactly("MSG_C", "MSG_B", "MSG_A"); // messageId 내림차순
        
        // messageId가 내림차순으로 정렬되었는지 별도 검증
        List<String> messageIds = result.stream()
                .map(ChatMessage::getMessageId)
                .toList();
        assertThat(messageIds)
                .as("messageId가 내림차순으로 정렬됨")
                .isSortedAccordingTo((id1, id2) -> id2.compareTo(id1));

        // 모든 메시지가 같은 시간인지 확인
        assertThat(result)
                .as("모든 메시지가 같은 생성 시간을 가짐")
                .extracting(ChatMessage::getCreatedAt)
                .containsOnly(sameTime);
    }

    @Test
    @DisplayName("페이지 크기 제한이 올바르게 작동한다")
    void findFirstPage_PageSizeLimit() {
        // Given
        List<ChatMessage> manyMessages = List.of(
                createChatMessage("MSG_1", testUser1Email, "메시지 1", baseTime.plusMinutes(1)),
                createChatMessage("MSG_2", testUser2Email, "메시지 2", baseTime.plusMinutes(2)),
                createChatMessage("MSG_3", testUser1Email, "메시지 3", baseTime.plusMinutes(3)),
                createChatMessage("MSG_4", testUser2Email, "메시지 4", baseTime.plusMinutes(4)),
                createChatMessage("MSG_5", testUser1Email, "메시지 5", baseTime.plusMinutes(5))
        );
        manyMessages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // When
        PageRequest smallPageRequest = PageRequest.of(0, 2);
        List<ChatMessage> result = chatMessageRepository.findFirstPage(testRoomId, smallPageRequest);

        // Then
        assertThat(result)
                .as("페이지 크기 제한 적용")
                .hasSize(2) // 요청한 크기만큼만 반환
                .hasSizeLessThanOrEqualTo(2);
                
        assertThat(result)
                .extracting(ChatMessage::getMessage)
                .containsExactly("메시지 5", "메시지 4"); // 최신 2개만
    }

    @Test
    @DisplayName("다중 사용자 환경에서 읽지 않은 메시지 카운트가 정확하다")
    void countUnreadMessages_MultipleUsers() {
        // Given
        String user3Email = "user3@test.com";
        ChatRoomAccount roomAccount3 = ChatRoomAccount.create(testRoomId, user3Email);
        entityManager.persistAndFlush(roomAccount3);

        // 각 사용자가 보낸 메시지들
        List<ChatMessage> messages = List.of(
                createChatMessage("MSG_FROM_USER1", testUser1Email, "user1 메시지", baseTime.plusMinutes(1)),
                createChatMessage("MSG_FROM_USER2", testUser2Email, "user2 메시지", baseTime.plusMinutes(2)),
                createChatMessage("MSG_FROM_USER3", user3Email, "user3 메시지", baseTime.plusMinutes(3))
        );
        messages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // When & Then
        // user1 관점에서는 user2와 user3의 메시지가 읽지 않음
        assertThat(chatMessageRepository.countUnreadMessages(testUser1Email))
                .as("user1의 읽지 않은 메시지 수")
                .isEqualTo(2);

        // user2 관점에서는 user1과 user3의 메시지가 읽지 않음
        assertThat(chatMessageRepository.countUnreadMessages(testUser2Email))
                .as("user2의 읽지 않은 메시지 수")
                .isEqualTo(2);

        // user3 관점에서는 user1과 user2의 메시지가 읽지 않음
        assertThat(chatMessageRepository.countUnreadMessages(user3Email))
                .as("user3의 읽지 않은 메시지 수")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("49개 메시지에서 커서 이전 조회 - 페이지 경계값 테스트")
    void findSliceBefore_49Messages() {
        // Given
        List<ChatMessage> messages = createSequentialMessages(49);
        messages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // 중간 지점의 메시지를 커서로 사용 (25번째 메시지)
        ChatMessage cursorMessage = messages.get(24); // 0-based index

        // When - 페이지 크기를 50으로 설정 (49개보다 큰 값)
        PageRequest pageRequest = PageRequest.of(0, 50);
        List<ChatMessage> result = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest
        );

        // Then
        assertThat(result)
                .as("49개 메시지 중 커서 이전 24개 조회")
                .hasSize(24) // 커서 이전의 모든 메시지
                .isSortedAccordingTo((m1, m2) -> m2.getCreatedAt().compareTo(m1.getCreatedAt())); // 최신순 정렬

        // 첫 번째 결과가 커서 바로 이전 메시지인지 확인
        assertThat(result.get(0))
                .as("첫 번째 결과는 커서 바로 이전 메시지")
                .satisfies(msg -> {
                    assertThat(msg.getCreatedAt()).isBefore(cursorMessage.getCreatedAt());
                    assertThat(msg.getMessage()).isEqualTo("메시지 24"); // 24번째 메시지
                });

        // 마지막 결과가 가장 오래된 메시지인지 확인
        assertThat(result.get(result.size() - 1))
                .as("마지막 결과는 가장 오래된 메시지")
                .satisfies(msg -> {
                    assertThat(msg.getMessage()).isEqualTo("메시지 1");
                });

        // 커서 메시지와 이후 메시지들이 포함되지 않았는지 확인
        assertThat(result)
                .as("커서 메시지와 이후 메시지는 제외")
                .noneMatch(msg -> msg.getMessageId().equals(cursorMessage.getMessageId()))
                .allSatisfy(msg -> 
                    assertThat(msg.getCreatedAt()).isBefore(cursorMessage.getCreatedAt())
                );
    }

    @Test
    @DisplayName("50개 메시지에서 커서 이전 조회 - 정확히 페이지 크기만큼")
    void findSliceBefore_50Messages() {
        // Given
        List<ChatMessage> messages = createSequentialMessages(50);
        messages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // 중간 지점의 메시지를 커서로 사용 (26번째 메시지)
        ChatMessage cursorMessage = messages.get(25); // 0-based index

        // When - 페이지 크기를 50으로 설정 (커서 이전 메시지와 동일)
        PageRequest pageRequest = PageRequest.of(0, 50);
        List<ChatMessage> result = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest
        );

        // Then
        assertThat(result)
                .as("50개 메시지 중 커서 이전 25개 조회")
                .hasSize(25) // 커서 이전의 모든 메시지
                .isSortedAccordingTo((m1, m2) -> m2.getCreatedAt().compareTo(m1.getCreatedAt()));

        // 페이지 크기보다 적은 결과가 반환되는지 확인 (더 이상 데이터가 없음을 의미)
        assertThat(result.size())
                .as("페이지 크기보다 적은 결과 (hasNext = false를 의미)")
                .isLessThan(50)
                .isEqualTo(25);

        // 연속성 검증 - 메시지 번호가 연속적인지 확인
        List<String> messageContents = result.stream()
                .map(ChatMessage::getMessage)
                .toList();
        
        assertThat(messageContents)
                .as("메시지 내용이 연속적으로 정렬됨")
                .containsExactly(
                        "메시지 25", "메시지 24", "메시지 23", "메시지 22", "메시지 21",
                        "메시지 20", "메시지 19", "메시지 18", "메시지 17", "메시지 16",
                        "메시지 15", "메시지 14", "메시지 13", "메시지 12", "메시지 11",
                        "메시지 10", "메시지 9", "메시지 8", "메시지 7", "메시지 6",
                        "메시지 5", "메시지 4", "메시지 3", "메시지 2", "메시지 1"
                );
    }

    @Test
    @DisplayName("51개 메시지에서 커서 이전 조회 - 페이지 크기보다 많은 데이터")
    void findSliceBefore_51Messages() {
        // Given
        List<ChatMessage> messages = createSequentialMessages(51);
        messages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // 중간 지점의 메시지를 커서로 사용 (26번째 메시지)
        ChatMessage cursorMessage = messages.get(25); // 0-based index

        // When - 페이지 크기를 20으로 설정 (커서 이전 메시지보다 적은 값)
        PageRequest pageRequest = PageRequest.of(0, 20);
        List<ChatMessage> result = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest
        );

        // Then
        assertThat(result)
                .as("51개 메시지 중 커서 이전 20개만 조회")
                .hasSize(20) // 페이지 크기만큼만 반환
                .isSortedAccordingTo((m1, m2) -> m2.getCreatedAt().compareTo(m1.getCreatedAt()));

        // 페이지 크기와 정확히 일치하는지 확인 (hasNext = true를 의미)
        assertThat(result.size())
                .as("페이지 크기와 정확히 일치 (hasNext = true를 의미)")
                .isEqualTo(20);

        // 가장 최신 20개의 메시지가 반환되는지 확인
        List<String> messageContents = result.stream()
                .map(ChatMessage::getMessage)
                .toList();

        assertThat(messageContents)
                .as("커서 이전의 가장 최신 20개 메시지")
                .containsExactly(
                        "메시지 25", "메시지 24", "메시지 23", "메시지 22", "메시지 21",
                        "메시지 20", "메시지 19", "메시지 18", "메시지 17", "메시지 16",
                        "메시지 15", "메시지 14", "메시지 13", "메시지 12", "메시지 11",
                        "메시지 10", "메시지 9", "메시지 8", "메시지 7", "메시지 6"
                )
                .doesNotContain("메시지 5", "메시지 4", "메시지 3", "메시지 2", "메시지 1"); // 오래된 메시지들은 제외

        // 두 번째 페이지를 요청해서 나머지 데이터 확인
        ChatMessage lastMessageFromFirstPage = result.get(result.size() - 1);
        PageRequest secondPageRequest = PageRequest.of(0, 20);
        List<ChatMessage> secondPageResult = chatMessageRepository.findSliceBefore(
                testRoomId,
                lastMessageFromFirstPage.getSequenceNumber(),
                secondPageRequest
        );

        assertThat(secondPageResult)
                .as("두 번째 페이지 결과")
                .hasSize(5) // 나머지 5개 메시지
                .extracting(ChatMessage::getMessage)
                .containsExactly("메시지 5", "메시지 4", "메시지 3", "메시지 2", "메시지 1");
    }

    @Test
    @DisplayName("커서 이전 조회에서 같은 시간 메시지들의 정렬 순서 확인")
    void findSliceBefore_SameTimeMessages() {
        // Given
        LocalDateTime sameTime = baseTime.plusMinutes(10);
        
        // 같은 시간에 여러 메시지 생성 (messageId로 정렬되어야 함)
        List<ChatMessage> sameTimeMessages = List.of(
                createChatMessage("MSG_A", testUser1Email, "메시지 A", sameTime),
                createChatMessage("MSG_B", testUser2Email, "메시지 B", sameTime),
                createChatMessage("MSG_C", testUser1Email, "메시지 C", sameTime),
                createChatMessage("MSG_D", testUser2Email, "메시지 D", sameTime),
                createChatMessage("MSG_E", testUser1Email, "메시지 E", sameTime)
        );
        sameTimeMessages.forEach(entityManager::persistAndFlush);

        // 커서 메시지 (다른 시간)
        ChatMessage cursorMessage = createChatMessage("CURSOR_MSG", testUser1Email, "커서 메시지", sameTime.plusMinutes(1));
        entityManager.persistAndFlush(cursorMessage);
        entityManager.flush();

        // When
        PageRequest pageRequest = PageRequest.of(0, 10);
        List<ChatMessage> result = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest
        );

        // Then
        assertThat(result)
                .as("같은 시간 메시지들이 messageId 내림차순으로 정렬")
                .hasSize(5)
                .extracting(ChatMessage::getMessageId)
                .isSortedAccordingTo((id1, id2) -> id2.compareTo(id1)); // messageId 내림차순

        // 모든 메시지가 같은 시간인지 확인
        assertThat(result)
                .as("모든 메시지가 같은 생성 시간")
                .extracting(ChatMessage::getCreatedAt)
                .containsOnly(sameTime);

        // 시간이 같을 때 messageId로 정렬되는지 세부 확인
        List<String> messageIds = result.stream()
                .map(ChatMessage::getMessageId)
                .toList();
        
        assertThat(messageIds.get(0))
                .as("첫 번째 메시지는 가장 큰 messageId")
                .isGreaterThan(messageIds.get(1));
        
        assertThat(messageIds.get(messageIds.size() - 1))
                .as("마지막 메시지는 가장 작은 messageId")
                .isLessThan(messageIds.get(messageIds.size() - 2));
    }

    // === Helper Methods ===

    private ChatMessage createChatMessage(String messageId, String senderEmail, String content, LocalDateTime createdAt) {
        return createChatMessage(messageId, senderEmail, content, createdAt, testRoomId);
    }

    private ChatMessage createChatMessage(String messageId, String senderEmail, String content, LocalDateTime createdAt, String roomId) {
        String fullMessageId = messageId + "_" + UUID.randomUUID().toString().substring(0, 8);
        return new ChatMessage(fullMessageId, roomId, senderEmail, content, 1L, createdAt);
    }

    private String createAnotherRoom() {
        String anotherRoomId = "ANOTHER_ROOM_" + UUID.randomUUID().toString().substring(0, 8);
        ChatRoom anotherRoom = ChatRoom.builder()
                .roomId(anotherRoomId)
                .title("다른 채팅방")

                .productId(2)
                .roomType(RoomType.GROUP)
                .build();
        entityManager.persistAndFlush(anotherRoom);

        ChatRoomAccount anotherRoomAccount = ChatRoomAccount.create(anotherRoomId, testUser1Email);
        entityManager.persistAndFlush(anotherRoomAccount);

        return anotherRoomId;
    }

    private String createEmptyRoom() {
        String emptyRoomId = "EMPTY_ROOM_" + UUID.randomUUID().toString().substring(0, 8);
        ChatRoom emptyRoom = ChatRoom.builder()
                .roomId(emptyRoomId)
                .title("빈 채팅방")

                .productId(3)
                .roomType(RoomType.DIRECT)
                .build();
        entityManager.persistAndFlush(emptyRoom);
        return emptyRoomId;
    }


    @Test
    @DisplayName("첫 페이지 메시지를 49개, 50개, 51개 메시지로 경계값 테스트")
    void findFirstPage_BoundaryValues() {
        // Given
        List<ChatMessage> messages49 = createSequentialMessages(49);
        messages49.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        List<ChatMessage> messages50 = createSequentialMessages(50);
        messages50.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        List<ChatMessage> messages51 = createSequentialMessages(51);
        messages51.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        // When & Then - 49개 메시지 테스트
        PageRequest pageRequest49 = PageRequest.of(0, 50);
        List<ChatMessage> result49 = chatMessageRepository.findFirstPage(testRoomId, pageRequest49);
        assertThat(result49).hasSize(49);

        // When & Then - 50개 메시지 테스트
        PageRequest pageRequest50 = PageRequest.of(0, 50);
        List<ChatMessage> result50 = chatMessageRepository.findFirstPage(testRoomId, pageRequest50);
        assertThat(result50).hasSize(50);

        // When & Then - 51개 메시지 테스트
        PageRequest pageRequest51 = PageRequest.of(0, 50);
        List<ChatMessage> result51 = chatMessageRepository.findFirstPage(testRoomId, pageRequest51);
        assertThat(result51).hasSize(50); // 최대 50개까지만 가져옴
    }

    @Test
    @DisplayName("커서 이전 메시지를 49개, 50개, 51개 경계값으로 조회 테스트")
    void findSliceBefore_BoundaryValues() {
        // Given
        List<ChatMessage> allMessages = createSequentialMessages(51);
        allMessages.forEach(entityManager::persistAndFlush);
        entityManager.flush();

        ChatMessage cursorMessage = allMessages.get(30); // 31번째 메시지를 커서로 사용

        // When & Then - 49개 메시지 테스트
        PageRequest pageRequest49 = PageRequest.of(0, 49);
        List<ChatMessage> result49 = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest49
        );
        assertThat(result49).hasSize(49);

        // When & Then - 50개 메시지 테스트
        PageRequest pageRequest50 = PageRequest.of(0, 50);
        List<ChatMessage> result50 = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest50
        );
        assertThat(result50).hasSize(30); // 커서 이전 메시지는 30개만 존재

        // When & Then - 51개 메시지 테스트
        PageRequest pageRequest51 = PageRequest.of(0, 51);
        List<ChatMessage> result51 = chatMessageRepository.findSliceBefore(
                testRoomId,
                cursorMessage.getSequenceNumber(),
                pageRequest51
        );
        assertThat(result51).hasSize(30); // 최대 메시지는 30개만 반환
    }

    // 테스트 데이터 생성 메서드
    private List<ChatMessage> createSequentialMessages(int count) {
        List<ChatMessage> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            LocalDateTime createdAt = baseTime.plusMinutes(i);
            String messageId = "MSG_" + i;
            String content = "메시지 " + i;
            String senderEmail = (i % 2 == 0) ? testUser2Email : testUser1Email;
            messages.add(createChatMessageWithSequence(messageId, senderEmail, content, (long) i, createdAt));
        }
        return messages;
    }

    private ChatMessage createChatMessageWithSequence(String messageId, String senderEmail, String content, Long sequenceNumber, LocalDateTime createdAt) {
        return createChatMessageWithSequence(messageId, senderEmail, content, sequenceNumber, createdAt, testRoomId);
    }

    private ChatMessage createChatMessageWithSequence(String messageId, String senderEmail, String content, Long sequenceNumber, LocalDateTime createdAt, String roomId) {
        String fullMessageId = messageId + "_" + UUID.randomUUID().toString().substring(0, 8);
        return new ChatMessage(fullMessageId, roomId, senderEmail, content, sequenceNumber, createdAt);
    }



}