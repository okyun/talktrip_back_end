package com.talktrip.talktrip.global.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.querydsl.collections.CollQueryFunctions.coalesce;

@Component
public class JWTUtil {

    private static final Logger logger = LoggerFactory.getLogger(JWTUtil.class);

    private static final ObjectMapper OM = new ObjectMapper();
    private static final Base64.Encoder B64U_ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64U_DEC = Base64.getUrlDecoder();
    private static final String HMAC_ALG = "HmacSHA256";

    private static byte[] secretBytes;

    // Secret 주입
    public JWTUtil(@Value("${jwt.secret-key}") String key) {
        try {
            if (key == null || key.trim().isEmpty()) {
                throw new IllegalArgumentException("JWT secret key is blank");
            }
            if (key.length() < 32) {
                throw new IllegalArgumentException("JWT secret key must be at least 32 characters");
            }
            secretBytes = key.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("JWT 키 초기화 실패: " + e.getMessage(), e);
        }
    }

    // JWT 생성 (분 단위 만료)
    public static String generateToken(Map<String, Object> valueMap, int min) {
        try {
            Map<String, Object> header = Map.of("alg", "HS256", "typ", "JWT");

            long nowSec = Instant.now().getEpochSecond();
            long expSec = nowSec + min * 60L;

            Map<String, Object> claims = new HashMap<>(valueMap == null ? Map.of() : valueMap);
            // subject 보장: sub 없으면 email/userId/username 순으로 대체
            if (!claims.containsKey("sub")) {
                Object sub = coalesce(claims.get("sub"), claims.get("email"), claims.get("userId"), claims.get("username"), claims.get("accountEmail"));
                if (sub != null) {
                    claims.put("sub", String.valueOf(sub));
                }
            }
            claims.putIfAbsent("iat", nowSec);
            claims.put("exp", expSec);

            String headerJson = OM.writeValueAsString(header);
            String payloadJson = OM.writeValueAsString(claims);

            String headerEnc = B64U_ENC.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
            String payloadEnc = B64U_ENC.encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));

            String signingInput = headerEnc + "." + payloadEnc;
            String signature = sign(signingInput, secretBytes);

            return signingInput + "." + signature;
        } catch (Exception e) {
            logger.error("Error generating JWT token: {}", e.getMessage());
            throw new RuntimeException("Error generating token", e);
        }
    }

    // JWT 검증 및 클레임 반환
    public static Map<String, Object> validateToken(String token) {
        try {
            if (token == null || token.trim().isEmpty()) {
                throw new IllegalArgumentException("Token is blank");
            }
            String[] parts = token.trim().split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Token format invalid");
            }

            String headerEnc = parts[0];
            String payloadEnc = parts[1];
            String signatureEnc = parts[2];

            String signingInput = headerEnc + "." + payloadEnc;
            String expectedSig = sign(signingInput, secretBytes);

            // 서명 검증(상수 시간 비교)
            if (!constantTimeEquals(signatureEnc, expectedSig)) {
                throw new IllegalArgumentException("Invalid signature");
            }

            // payload 파싱
            byte[] payloadBytes = B64U_DEC.decode(payloadEnc);
            Map<String, Object> claims = OM.readValue(payloadBytes, new TypeReference<Map<String, Object>>() {});

            // 만료 확인(exp는 초 단위 NumericDate)
            Object expObj = claims.get("exp");
            if (expObj == null) {
                throw new IllegalArgumentException("Missing exp");
            }
            long expSec;
            if (expObj instanceof Number) {
                expSec = ((Number) expObj).longValue();
            } else {
                expSec = Long.parseLong(String.valueOf(expObj));
            }
            long nowSec = Instant.now().getEpochSecond();
            if (nowSec > expSec) {
                throw new IllegalArgumentException("Token expired");
            }

            return claims;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("JWT validation error", e);
        }
    }

    private static String sign(String signingInput, byte[] secret) throws Exception {
        SecretKey key = new SecretKeySpec(secret, HMAC_ALG);
        Mac mac = Mac.getInstance(HMAC_ALG);
        mac.init(key);
        byte[] sig = mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8));
        return B64U_ENC.encodeToString(sig);
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        if (a.length() != b.length()) return false;
        int result = 0;
        for (int i = 0; i < a.length(); i++) {
            result |= a.charAt(i) ^ b.charAt(i);
        }
        return result == 0;
    }

    /**
     * 주어진 claim 키 목록 중에서, 첫 번째로 유효한(문자열이면 non-blank, 그 외는 non-null) 값을 String으로 반환합니다.
     * 없으면 null을 반환하거나, 필요하다면 IllegalStateException을 던지도록 바꾸세요.
     */
    public static String constantTimeEquals(Map<String, Object> claims, String... keys) {
        if (claims == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            if (key == null) continue;
            Object value = claims.get(key);
            if (value == null) continue;

            if (value instanceof CharSequence) {
                String s = value.toString().trim();
                if (!s.isEmpty()) {
                    return s;
                }
            } else {
                // 숫자/불린/기타 타입도 안전하게 문자열화
                return value.toString();
            }
        }
        return null; // 또는 throw new IllegalStateException("필수 claim 없음");
    }
}
