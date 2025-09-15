package com.talktrip.talktrip.domain.chat.repository;

import com.talktrip.talktrip.domain.chat.dto.response.ChatMessageDto;
import com.talktrip.talktrip.domain.chat.entity.ChatMessage;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface ChatMessageRepository extends JpaRepository<ChatMessage, String> {

    @Query(value = """
      SELECT COUNT(*)
      FROM chatting_message_history_tab
      WHERE room_id = :roomId
        AND created_at > (
          SELECT created_at
          FROM chatting_message_history_tab
          WHERE message_id = (
            SELECT last_read_message_id
            FROM chatting_room_users_tab
            WHERE account_email = :userId
              AND room_id = :roomId
          )
        )
     """, nativeQuery = true)
    int countUnreadMessagesByRoomId(
            @Param("roomId") String roomId,
            @Param("userId") String userId
    );
    @Query(value = """
     SELECT COUNT(*) AS unread_room_count
        FROM chatting_room_account_tab crmt
        WHERE crmt.account_email  = :userId
          AND EXISTS (
            SELECT 1
            FROM chatting_message_history_tab msg
            WHERE msg.room_id = crmt.room_id
              AND msg.created_at > (
                SELECT sub.created_at
                FROM chatting_message_history_tab sub
                WHERE sub.message_id = crmt.last_read_message_id
              )
          )
   """, nativeQuery = true)
    int countUnreadMessagesRooms(
            @Param("userId") String userId
    );

    @Query(value = """
     SELECT IFNULL(SUM(not_read_message_count), 0) AS total_unread_message_count
     FROM (
         SELECT COUNT(*) AS not_read_message_count
         FROM chatting_room_account_tab crmt
         JOIN chatting_room_tab crt ON crt.room_id = crmt.room_id
         JOIN chatting_message_history_tab msg ON msg.room_id = crmt.room_id
         WHERE crmt.account_email = :userId
           AND (
               crmt.last_member_read_time IS NULL
               OR msg.created_at > crmt.last_member_read_time
           )
           AND msg.account_email != :userId -- ✅ 나 자신이 보낸 메시지는 제외
         GROUP BY crmt.room_id
     ) AS unread_counts;
   """, nativeQuery = true)
    int countUnreadMessages(
            @Param("userId") String userId
    );
    List<ChatMessageDto> findByRoomIdOrderByCreatedAt(String userId);

    @Query(value = """
      SELECT COUNT(*)
      FROM chatting_message_history_tab cmht
      WHERE cmht.room_id = :roomId
        AND cmht.account_email != :memberId       -- 상대가 보낸 것만
        AND cmht.created_at >
            COALESCE(
              (SELECT crat.last_member_read_time
               FROM chatting_room_account_tab crat
               WHERE crat.room_id = :roomId
                 AND crat.account_email = :memberId ),  -- 내가 마지막으로 읽은 시각
              '1970-01-01 00:00:00'
            )
""", nativeQuery = true)
    int countUnreadMessagesByRoomIdAndMemberId(
            @Param("roomId") String roomId,
            @Param("memberId") String memberId
    );

    @Query(value = """
     select crat.account_email
           from chatting_room_account_tab crat
           where crat.room_id  = :roomId
           and crat.account_email  != :userId
           limit 1
   """, nativeQuery = true)
    String getOtherMemberIdByRoomIdandUserId(
            @Param("userId") String userId,
            @Param("roomId") String roomId
    );


    // 첫 페이지(커서 없음): 최신 50개 - 시퀀스 기반 정렬
    @Query("""
        select m
        from ChatMessage m
        where m.roomId = :roomId
        order by m.sequenceNumber desc, m.createdAt desc, m.messageId desc
    """)
    List<ChatMessage> findFirstPage(
            @Param("roomId") String roomId,
            PageRequest pageable
    );

    // 커서 이전(더 과거) 50개: 시퀀스 기반 정렬
    @Query("""
    select m
    from ChatMessage m
    where m.roomId = :roomId
      and m.sequenceNumber < :cursorSequenceNumber
    order by m.sequenceNumber desc, m.createdAt desc, m.messageId desc
""")
    List<ChatMessage> findSliceBefore(
            @Param("roomId") String roomId,
            @Param("cursorSequenceNumber") Long cursorSequenceNumber,
            PageRequest pageable
    );

    // 특정 채팅방의 마지막 sequenceNumber 조회
    @Query("""
        SELECT COALESCE(MAX(m.sequenceNumber), 0)
        FROM ChatMessage m
        WHERE m.roomId = :roomId
    """)
    Long findMaxSequenceNumberByRoomId(@Param("roomId") String roomId);
}
