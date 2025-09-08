package com.talktrip.talktrip.domain.chat.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.talktrip.talktrip.domain.chat.dto.request.ChatRoomRequestDto;
import com.talktrip.talktrip.domain.chat.dto.response.*;
import com.talktrip.talktrip.domain.chat.service.ChatService;
import com.talktrip.talktrip.global.dto.SliceResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("ChatApiController 테스트")
class ChatApiControllerTest {

    @Mock
    private ChatService chatService;

    @InjectMocks
    private ChatApiController chatApiController;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(chatApiController).build();
        objectMapper = new ObjectMapper();
    }

    @Test
    @DisplayName("채팅방 접속 - POST /api/chat")
    void enterChatRoom() throws Exception {
        // When & Then
        mockMvc.perform(post("/api/chat"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("내 채팅 목록 조회 - GET /api/chat/me/chatRooms")
    void getMyChats() throws Exception {
        // Given
        List<ChatRoomDTO> mockRooms = Arrays.asList(
                new ChatRoomDTO("ROOM_001", "RA_001", LocalDateTime.now(), LocalDateTime.now(), "방1", "메시지1", 1L, null),
                new ChatRoomDTO("ROOM_002", "RA_002", LocalDateTime.now().minusDays(1), LocalDateTime.now().minusDays(1), "방2", "메시지2", 2L, null)
        );
       // when(chatService.getRooms("test@example.com")).thenReturn(mockRooms);

        // When & Then
        mockMvc.perform(get("/api/chat/me/chatRooms")
                        .principal(() -> "test@example.com"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].roomId").value("ROOM_001"))
                .andExpect(jsonPath("$[1].roomId").value("ROOM_002"));

        //verify(chatService).getRooms("test@example.com");
    }

    @Test
    @DisplayName("안읽은 모든 채팅 개수 조회 - GET /api/chat/countALLUnreadMessages")
    void getCountAllUnreadMessages() throws Exception {
        // Given
        when(chatService.getCountAllUnreadMessages("test@example.com")).thenReturn(15);

        // When & Then
        mockMvc.perform(get("/api/chat/countALLUnreadMessages")
                        .principal(() -> "test@example.com"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(15));

        verify(chatService).getCountAllUnreadMessages("test@example.com");
    }

    @Test
    @DisplayName("채팅방 입장 또는 생성 - POST /api/chat/rooms/enter")
    void enterOrCreateRoom() throws Exception {
        // Given
        ChatRoomRequestDto requestDto = new ChatRoomRequestDto();
        requestDto.setSellerAccountEmail("seller@example.com");
        requestDto.setProductId(1);
        when(chatService.enterOrCreateRoom(any(), eq(requestDto))).thenReturn("ROOM_001");

        // When & Then
        mockMvc.perform(post("/api/chat/rooms/enter")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(requestDto))
                        .principal(() -> "test@example.com"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.roomId").value("ROOM_001"));

        verify(chatService).enterOrCreateRoom(any(), eq(requestDto));
    }

    @Test
    @DisplayName("채팅방 나가기 - PATCH /api/chat/me/chatRooms/{roomId}")
    void leaveChatRoom() throws Exception {
        // When & Then
        mockMvc.perform(patch("/api/chat/me/chatRooms/ROOM_001")
                        .principal(() -> "test@example.com"))
                .andExpect(status().isNoContent());

        verify(chatService).markChatRoomAsDeleted("test@example.com", "ROOM_001");
    }

    @Test
    @DisplayName("채팅방 상세 조회 - GET /api/chat/me/chatRooms/{roomId}")
    void getChatRoom() throws Exception {
        // Given
        String roomId = "ROOM_001";
        ChatRoomDetailDto mockRoom = new ChatRoomDetailDto(
                "ROOM_001", "방1", 1, "owner@example.com",
                LocalDateTime.now(), 2, Arrays.asList("user1@example.com", "user2@example.com")
        );
        
        when(chatService.getRoomDetail(roomId, "test@example.com")).thenReturn(mockRoom);
        
        // When & Then
        mockMvc.perform(get("/api/chat/me/chatRooms/{roomId}", roomId)
                        .param("includeMessages", "true")
                        .param("limit", "50")
                        .principal(() -> "test@example.com"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.room.roomId").value("ROOM_001"))
                .andExpect(jsonPath("$.room.title").value("방1"));
    }

    @Test
    @DisplayName("채팅방 메시지 조회 - GET /api/chat/me/chatRooms/{roomId}/messages")
    void getRoomMessages() throws Exception {
        // Given
        String roomId = "ROOM_001";
        ChatMemberRoomWithMessageDto mockMessage = new ChatMemberRoomWithMessageDto(
                "MSG_001", "ROOM_001", "user1@example.com", "안녕하세요", 1L, LocalDateTime.now(), null
        );
        
        SliceResponse<ChatMemberRoomWithMessageDto> mockResponse = new SliceResponse<>(
                Arrays.asList(mockMessage), null, false
        );
        
        when(chatService.getRoomChattingHistoryAndMarkAsRead(roomId, "test@example.com", 50, null))
                .thenReturn(mockResponse);
        
        // When & Then
        mockMvc.perform(get("/api/chat/me/chatRooms/{roomId}/messages", roomId)
                        .param("limit", "50")
                        .principal(() -> "test@example.com"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].messageId").value("MSG_001"))
                .andExpect(jsonPath("$.items[0].message").value("안녕하세요"));
    }
}
