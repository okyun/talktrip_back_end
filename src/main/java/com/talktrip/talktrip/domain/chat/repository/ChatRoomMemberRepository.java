package com.talktrip.talktrip.domain.chat.repository;

import com.talktrip.talktrip.domain.chat.entity.ChatRoomAccount;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ChatRoomMemberRepository extends JpaRepository<ChatRoomAccount, String>{

    @Modifying
    @Transactional
    @Query("UPDATE ChatRoomAccount crm SET crm.lastMemberReadTime = :currentTime " +
            "WHERE crm.roomId = :roomId AND crm.accountEmail = :accountEmail")
    int updateLastReadTime(@Param("roomId") String roomId,
                           @Param("accountEmail") String accountEmail,
                           @Param("currentTime") java.time.LocalDateTime currentTime);

    @Query(value = """
        SELECT crm1.room_id
        FROM chatting_room_account_tab crm1
        JOIN chatting_room_account_tab crm2 ON crm1.room_id = crm2.room_id
        join chatting_room_tab crt on crt.room_id = crm1.room_id and crt.room_type ='DIRECT'
        WHERE crm1.account_email = :buyerId
          AND crm2.account_email = :sellerId
        LIMIT 1
    """, nativeQuery = true)
    Optional<String> findRoomIdByBuyerIdAndSellerId(
            @Param("buyerId") String buyerId,
            @Param("sellerId") String sellerId
    );

    @Modifying
    @Transactional
    @Query("UPDATE ChatRoomAccount crm " +
            "SET crm.isDel = :isDel " +
            "WHERE crm.accountEmail = :memberId AND crm.roomId = :roomId")
    void updateIsDelByMemberIdAndRoomId(@Param("memberId") String memberId,
                                        @Param("roomId") String roomId,
                                        @Param("isDel") int isDel);
    @Modifying
    @Transactional
    @Query("UPDATE ChatRoomAccount crm SET crm.isDel = 0 WHERE crm.roomId = :roomId")
    void resetIsDelByRoomId(@Param("roomId") String roomId);


    @Query("""
        select m.lastMemberReadTime
        from ChatRoomAccount m
        where m.roomId = :roomId and m.accountEmail = :email
    """)
    Optional<java.time.LocalDateTime> findMyLastReadAt(
            @Param("roomId") String roomId,
            @Param("email") String email
    );

    @Query("""
        select count(m)
        from ChatRoomAccount m
        where m.roomId = :roomId
    """)
    int countMembers(@Param("roomId") String roomId);

    @Query("""
        select m.accountEmail
        from ChatRoomAccount m
        where m.roomId = :roomId
        order by m.accountEmail asc
    """)
    List<String> findParticipantEmails(@Param("roomId") String roomId);


    List<ChatRoomAccount> findAllAccountEmailsByRoomId(String roomId);


    Boolean existsByRoomIdAndAccountEmail(String roomId, String accountEmail);
}


