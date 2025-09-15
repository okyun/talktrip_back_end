package com.talktrip.talktrip.global.security.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.talktrip.talktrip.domain.member.entity.Member;
import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.global.security.CustomMemberDetails;
import com.talktrip.talktrip.global.util.JWTUtil;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Optional;
@RequiredArgsConstructor
@Slf4j
public class JWTCheckFilter extends OncePerRequestFilter {

    private final JWTUtil jwtUtil;
    private final MemberRepository memberRepository;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String uri = request.getRequestURI();
        log.info("[JWTCheckFilter] 실행됨 - URI: {}", uri);

        String authHeader = request.getHeader("Authorization");
        String accessToken = null;

        // 1. 헤더에서 토큰 추출
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            accessToken = authHeader.substring(7);
        } else if (request.getCookies() != null) {
            // 2. 쿠키에서 토큰 추출
            for (Cookie cookie : request.getCookies()) {
                if (cookie.getName().equals("accessToken")) {
                    accessToken = cookie.getValue();
                    break;
                }
                if (cookie.getName().equals("member")) {
                    try {
                        String decoded = URLDecoder.decode(cookie.getValue(), "UTF-8");
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> memberMap = mapper.readValue(decoded, Map.class);
                        if (memberMap.containsKey("accessToken")) {
                            accessToken = (String) memberMap.get("accessToken");
                            break;
                        }
                    } catch (Exception e) {
                        log.error("member 쿠키 파싱 실패: {}", e.getMessage());
                    }
                }
            }
        }

        // 비로그인 허용 경로일 경우 토큰 없이도 통과시킴
        if (accessToken == null) {
            if (isPublicPath(uri)) {
                log.info("[JWTCheckFilter] 토큰 없음 → 공개 경로, SecurityContext 설정 없이 통과");
                filterChain.doFilter(request, response);
                return;
            } else {
                log.warn("[JWTCheckFilter] 인증 필요 경로에 토큰 없음 → 401 반환");
                respondWithUnauthorized(response, "MISSING_ACCESS_TOKEN");
                return;
            }
        }

        try {
            // JWT 검증
            Map<String, Object> claims = jwtUtil.validateToken(accessToken);
            String email = claims.get("email").toString();

            Optional<Member> memberOptional = memberRepository.findByAccountEmail(email);
            if (memberOptional.isEmpty()) {
                respondWithUnauthorized(response, "MEMBER_NOT_FOUND");
                return;
            }

            Member member = memberOptional.get();
            CustomMemberDetails customMemberDetails = new CustomMemberDetails(member);

            UsernamePasswordAuthenticationToken authToken =
                    new UsernamePasswordAuthenticationToken(customMemberDetails, null, customMemberDetails.getAuthorities());

            SecurityContextHolder.getContext().setAuthentication(authToken);
            log.info("[JWTCheckFilter] 인증 완료 - memberId: {}", customMemberDetails.getId());

        } catch (Exception e) {
            // JWT 검증 실패 → 비로그인 허용 경로면 통과, 아니면 401
            if (isPublicPath(uri)) {
                log.info("[JWTCheckFilter] JWT 검증 실패, 하지만 공개 URI → 비로그인으로 통과");
                filterChain.doFilter(request, response);
                return;
            }

            log.warn("[JWTCheckFilter] JWT 검증 실패: {}", e.getMessage());
            respondWithUnauthorized(response, "INVALID_OR_EXPIRED_TOKEN");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private boolean isPublicPath(String uri) {
        return uri.equals("/api/member/kakao") ||
                uri.equals("/api/member/kakao-login-url") ||

                uri.startsWith("/swagger") ||
                uri.startsWith("/swagger-ui") ||
                uri.startsWith("/v3/api-docs") ||
                uri.startsWith("/swagger-resources") ||
                uri.startsWith("/webjars") ||

                // Actuator 엔드포인트 허용
                uri.startsWith("/api/actuator/") ||
                uri.startsWith("/actuator/") ||
                // 상품 조회 및 AI 검색, 리뷰 목록은 비로그인 허용
                (uri.startsWith("/api/products") && !uri.contains("/like")) ||
                uri.startsWith("/api/ai-search") ||
                uri.startsWith("/api/reviews")||
                uri.startsWith("/api/products") ||
                uri.startsWith("/api/orders")||
                uri.startsWith("/api/chat/") ||  // 채팅 API 제외
                uri.startsWith("/api/alarm/") ||  // 알림 API 제외 (개발 단계)
                // WebSocket 관련 모든 경로 허용
                // 핸드셰이크 공개 유지(옵션 A/B-1 선택 시)
                uri.startsWith("/ws")
                || uri.startsWith("/ws-info")
                || uri.startsWith("/sockjs-node");
    }


    private void respondWithUnauthorized(HttpServletResponse response, String message) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        PrintWriter writer = response.getWriter();
        new ObjectMapper().writeValue(writer, Map.of("error", message));
        writer.flush();
    }
}


