package com.talktrip.talktrip.global.webSocket.stomp;

import com.talktrip.talktrip.domain.member.entity.Member;
import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.global.util.JWTUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.profiles.active", havingValue = "!test", matchIfMissing = true)
public class JwtStompChannelInterceptor implements ChannelInterceptor {

    private final JWTUtil jwtProvider;
    private final MemberRepository memberRepository;

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor acc = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (acc == null) return message;

        StompCommand cmd = acc.getCommand();
        if (cmd == null) return message;
        //simpDestination -> /topic/chat/room/ROOM_0f7871a736 이렇게 config에 지정된 topic 만 subscribe
        // CONNECT 또는 STOMP에서만 토큰을 강제, ***SUBSCRIBE도 추가 해줘야함...!!!
        if (StompCommand.CONNECT.equals(cmd) || StompCommand.STOMP.equals(cmd)) {
            String auth = firstNativeHeaderIgnoreCase(acc, "Authorization");
            if (auth == null || !auth.startsWith("Bearer ")) {
                throw new AccessDeniedException("Missing or invalid Authorization header");
            }

            String token = auth.substring(7).trim();

            final Map<String, Object> claims;
            try {
                claims = JWTUtil.validateToken(token); // 유효하면 claims 반환, 아니면 예외
            } catch (Exception e) {
                throw new AccessDeniedException("Invalid JWT");
            }

            String accountEmail = JWTUtil.constantTimeEquals(
                    claims, "sub", "email", "userId", "username", "accountEmail"
            );
            if (accountEmail == null || accountEmail.isBlank()) {
                throw new AccessDeniedException("Missing subject claim");
            }

            // 존재 사용자 확인(필요 시)
            Member member = memberRepository.findByAccountEmail(accountEmail)
                    .orElseThrow(() -> new AccessDeniedException("User not found"));

            // 핵심: principal을 Member가 아닌 "accountEmail 문자열"로 세팅
            UsernamePasswordAuthenticationToken authentication =
                    new UsernamePasswordAuthenticationToken(accountEmail, null, List.of());

            // STOMP 세션/메시지에 사용자 주입
            acc.setUser(authentication);

            // 세션에도 저장해 두었다가 SEND 시 복구
            Map<String, Object> sessionAttributes = acc.getSessionAttributes();
            if (sessionAttributes != null) {
                sessionAttributes.put("wsPrincipal", authentication);
            }

            return message;
        }

        // 메시지 전송(SEND) 시에는 STOMP 세션에서만 복구/확인
        if (StompCommand.SEND.equals(cmd)) {
            Principal principal = acc.getUser();

            if (principal == null) {
                Map<String, Object> sessionAttributes = acc.getSessionAttributes();
                if (sessionAttributes != null) {
                    Object saved = sessionAttributes.get("wsPrincipal");
                    if (saved instanceof Principal) {
                        principal = (Principal) saved;
                    }
                }
            }

            if (principal == null) {
                throw new AccessDeniedException("Unauthenticated WebSocket session");
            }

            // 이후 핸들러에서 Principal 주입 보장
            acc.setUser(principal);
        }

        return message;
    }

    private String firstNativeHeaderIgnoreCase(StompHeaderAccessor acc, String key) {
        if (acc == null || key == null) return null;
        List<String> exact = acc.getNativeHeader(key);
        if (exact != null && !exact.isEmpty()) return exact.get(0);
        List<String> lower = acc.getNativeHeader(key.toLowerCase());
        if (lower != null && !lower.isEmpty()) return lower.get(0);
        List<String> upper = acc.getNativeHeader(key.toUpperCase());
        if (upper != null && !upper.isEmpty()) return upper.get(0);
        return null;
    }
}
