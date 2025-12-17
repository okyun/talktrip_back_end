package com.talktrip.talktrip.domain.member.service;

import com.talktrip.talktrip.domain.member.dto.response.MemberResponseDTO;
import com.talktrip.talktrip.domain.member.entity.Member;
import com.talktrip.talktrip.domain.member.enums.Gender;
import com.talktrip.talktrip.domain.member.enums.MemberRole;
import com.talktrip.talktrip.domain.member.enums.MemberState;
import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.global.util.RandomNicknameGenerator;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.LocalDate;
import java.util.Map;

@Service
@Transactional
@RequiredArgsConstructor
public class KakaoAuthService {

    private static final Logger logger = LoggerFactory.getLogger(KakaoAuthService.class);

    private final PasswordEncoder passwordEncoder;
    private final MemberRepository memberRepository;

    @Value("${spring.security.oauth2.client.provider.kakao.authorization-uri}")
    private String authorizationUri; //인가코드 요청 Uri
    @Value("${spring.security.oauth2.client.provider.kakao.user-info-uri}")
    private String userInfoUri; //카카오 유저 정보를 얻기 위한 주소 링크 가져오기
    @Value("${spring.security.oauth2.client.provider.kakao.token-uri}")
    private String tokenUri; // AccessToken 요청 주소
    @Value("${spring.security.oauth2.client.registration.kakao.client-id}")
    private String clientId; //REST API Key
    @Value("${spring.security.oauth2.client.registration.kakao.client-secret}")
    private String clientSecret; // Client Secret Key
    @Value("${spring.security.oauth2.client.registration.kakao.redirect-uri}")
    private String redirectUri; // Redirect URI

    public String getKakaoAuthorizeUrl() {
        return UriComponentsBuilder
                .fromHttpUrl(authorizationUri)
                .queryParam("client_id", clientId)
                .queryParam("redirect_uri", redirectUri)
                .queryParam("response_type", "code")
                .toUriString();
    }

    public MemberResponseDTO loginWithKakao(String code) {
        logger.info("Kakao login code : " + code);
        // 1. 토큰 요청
        RestTemplate rest = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("grant_type", "authorization_code");
        params.add("client_id", clientId);
        params.add("client_secret", clientSecret);
        params.add("redirect_uri", redirectUri);
        params.add("code", code);

        HttpEntity<MultiValueMap<String, String>> tokenRequest = new HttpEntity<>(params, headers);
        Map tokenRes = rest.postForObject(tokenUri, tokenRequest, Map.class);

        logger.info("tokenRequest: {}", tokenRequest);
        logger.info("tokenRes: {}",tokenRes);
        String accessToken = (String) tokenRes.get("access_token");

        // 2. 사용자 정보 요청
        HttpHeaders profileHeaders = new HttpHeaders();
        profileHeaders.setBearerAuth(accessToken);
        HttpEntity<?> profileReq = new HttpEntity<>(profileHeaders);

        ResponseEntity<Map> profileRes = rest.exchange(
                userInfoUri,
                HttpMethod.GET,
                profileReq,
                Map.class
        );

        Map<String, Object> kakaoAccount = (Map<String, Object>) profileRes.getBody().get("kakao_account");
        Map<String, Object> profile = (Map<String, Object>) kakaoAccount.get("profile");

        logger.info("kakaoAccount : {}", kakaoAccount);
        logger.info("profile: {}", profile);

        String email = (String) kakaoAccount.get("email");
        String nickname = RandomNicknameGenerator.generate();
        String name = (String) kakaoAccount.get("name");
        String gender = (String) kakaoAccount.get("gender");
        String birthDay = (String) kakaoAccount.get("birthday");
        Integer birthYear = Integer.valueOf((String) kakaoAccount.get("birthyear"));
        String phone = (String) kakaoAccount.get("phone_number");

        Boolean isFemale = "female".equalsIgnoreCase(gender);

        // 3. 회원 저장 또는 조회
        Member member = memberRepository.findByAccountEmail(email)
                .orElseGet(() -> memberRepository.save(
                        Member.builder()
                                .accountEmail(email)
                                .nickname(nickname)
                                .name(name)
                                .memberRole(MemberRole.U)
                                .memberState(MemberState.A)
                                .gender(isFemale != null && isFemale ? Gender.F : Gender.M)
                                .birthday(LocalDate.of(
                                        birthYear,
                                        Integer.parseInt(birthDay.substring(0, 2)),
                                        Integer.parseInt(birthDay.substring(2))
                                ))
                                .phoneNum(phone)
                                .build()
                ));

        return new MemberResponseDTO(member);
    }
}
