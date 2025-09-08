package com.talktrip.talktrip.domain.chat.controller;

import com.talktrip.talktrip.domain.chat.dto.request.ChatRoomRequestDto;
import com.talktrip.talktrip.domain.chat.dto.response.*;
import com.talktrip.talktrip.domain.chat.service.ChatService;
import com.talktrip.talktrip.global.dto.SliceResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;
import java.util.List;
import java.util.Map;

@Tag(name = "Chat", description = "채팅 관련 API")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/chat")
public class ChatApiController {

    private final ChatService chatService;

    @Operation(summary = "채팅방 접속")
    @PostMapping
    public void enterChatRoom() {}


    @Operation(summary = "내 채팅 목록 (페이지네이션)")
    @GetMapping("/me/chatRooms")
    public SliceResponse<ChatRoomDTO> getMyChats(
            Principal principal,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) String cursor
    ) {
        String accountEmail = principal.getName();
        return chatService.getRooms(accountEmail, limit, cursor);
    }

    @Operation(summary = "내 채팅 목록 (전체 - 기존 호환성)")
    @GetMapping("/me/chatRooms/all")
    public List<ChatRoomDTO> getAllMyChats(
            Principal principal,
            @RequestParam(required = false) String roomType) {
        String accountEmail = principal.getName();
        return chatService.getAllRooms(accountEmail, roomType);
    }

    @Operation(summary = "채팅방 메시지 조회 (includeMessages=true: 첫 페이지, false: 무한스크롤)")
    @GetMapping("/me/chatRooms/{roomId}/messages")
    public SliceResponse<ChatMemberRoomWithMessageDto> getRoomMessages(
            @PathVariable String roomId,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "false") boolean includeMessages,
            Principal principal
    ) {
        String email = principal.getName();
        
        // includeMessages=true이면 첫 페이지 (cursor 무시)
        // includeMessages=false이면 무한 스크롤 (cursor 사용)
        String effectiveCursor = includeMessages ? null : cursor;
        
        return chatService.getRoomChattingHistoryAndMarkAsRead(
                roomId,
                email,
                limit,
                effectiveCursor
        );
    }
    @Operation(summary = "안읽은 모든 채팅갯수")
    @GetMapping("/countALLUnreadMessages")
    public Map<String, Integer> getCountAllUnreadMessages(Principal principal) {
        int count = chatService.getCountAllUnreadMessages(principal.getName());//실시간으로 나오게
        return Map.of("count", count);
    }
    @Operation(summary = "채팅방 입장 또는 생성")
    @PostMapping("/rooms/enter")
    public ResponseEntity<ChatRoomResponseDto> enterOrCreateRoom(Principal principal,
                                                                 @RequestBody ChatRoomRequestDto chatRoomRequestDto) {
        String roomId = chatService.enterOrCreateRoom(principal,chatRoomRequestDto);
        return ResponseEntity.ok(new ChatRoomResponseDto(roomId));
    }
    @Operation(summary = "채팅방 읽음 처리 (notReadMessageCount 초기화)")
    @PatchMapping("/me/chatRooms/{roomId}/markAsRead")
    public ResponseEntity<Void> markRoomAsRead(Principal principal, @PathVariable String roomId) {
        chatService.markRoomAsRead(principal.getName(), roomId);
        return ResponseEntity.noContent().build(); // 204 No Content
    }

    @Operation(summary = "채팅방 나가기(삭제 처리)")
    @PatchMapping("/me/chatRooms/{roomId}")
    public ResponseEntity<Void> leaveChatRoom(Principal principal,@PathVariable String roomId) {
        chatService.markChatRoomAsDeleted(principal.getName(), roomId);
        return ResponseEntity.noContent().build(); // 204 No Content
    }

}
