package com.talktrip.talktrip.domain.chat.controller;


import com.talktrip.talktrip.domain.chat.dto.response.ChatMessagePush;
import com.talktrip.talktrip.global.util.SeoulTimeUtil;
import com.talktrip.talktrip.global.redis.RedisMessageBroker;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/test")
public class TestRedisController {

    private final RedisMessageBroker redisMessageBroker;

    @PostMapping("/publish")
    public ResponseEntity<String> publishTestMessage(
            @RequestParam String channel,
            @RequestParam String roomId,
            @RequestParam String message
    ) {
        ChatMessagePush pushMessage = ChatMessagePush.builder()
                .messageId("TEST_MSG_ID")
                .roomId(roomId)
                .sender("testuser@test.com")
                .senderName("Tester")
                .message(message)
                .createdAt(SeoulTimeUtil.nowAsString())
                .build();

        redisMessageBroker.publish(channel, pushMessage);
        return ResponseEntity.ok("[TestRedisController] Test 메시지가 Redis로 발행되었습니다.");
    }
}
